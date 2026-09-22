# backlogger-reprocess

A single Maven module (Java 21, one shared `pom.xml`/fat jar) holding seven
independent CLI tools, each with its own `main()`:

| Tool | Entry point | Purpose |
|---|---|---|
| Backlogger reprocess | `com.smarsh.backlogger.Main` (the jar's default `Main-Class`, run via `java -jar ...`) | Reprocess S3 keys via the backlogger `replayKeys` API, skipping keys already in ES |
| Duplicate source-id checker | `com.smarsh.backlogger.DuplicateSourceIdChecker` (run via `java -cp ... <class>`) | For each sourceId in a CSV, classify it as unique/duplicate/zero-count via an ES `_count` query |
| Duplicate key checker | `com.smarsh.keycount.Main` (run via `java -cp ... <class>`) | For each key in a CSV, count how many times it appears in ES — see its own README in `src/main/java/com/smarsh/keycount/` |
| Athena year key-count | `com.smarsh.athena.Main` (run via `java -cp ... <class>`) | Per-year Athena query counting `DISTINCT key` (or any field via `--count-field`), written to a local CSV |
| ES vs. Athena reconciliation | `com.smarsh.reconcile.Main` (run via `java -cp ... <class>`) | Recursive month→second drill-down comparing ES/Athena unique-key counts, writing outstanding keys to a CSV — see its own README in `src/main/java/com/smarsh/reconcile/` |
| Source-id → key → Athena lookup | `com.smarsh.sourcelookup.SourceIdAthenaLookup` (run via `java -cp ... <class>`) | For each sourceId in a CSV: finds its ES key, derives the Athena quadrimester from that key's date, then confirms via a narrowed `COUNT(*)` — see its own README in `src/main/java/com/smarsh/sourcelookup/` |
| Quadrimester Athena migration | `com.smarsh.migration.QuadrimesterMigrator` (run via `java -cp ... <class>`) | Per-quadrimester dedup + verify + UNLOAD migration of `tier2_migration_duplicate_stage2` data to a permanent S3 destination, split into 4 monthly steps to avoid Athena query exhaustion — see its own README in `src/main/java/com/smarsh/migration/` |

Only the first tool is the jar's default `Main-Class` (`java -jar
target\backlogger-reprocess-1.0.0.jar ...`); the rest are run by
specifying their fully-qualified class name with `-cp` instead — same fat
jar, same dependencies, different entry point. See each tool's own section
below (or its package-level README, for the two that have one) for its
specific config/usage.

---

# Tool 1: Backlogger reprocess (`com.smarsh.backlogger.Main`)

Reprocesses S3 keys via the backlogger `replayKeys` API, skipping keys
that are already present in Elasticsearch.

## What it does

1. Streams S3 keys from `swfaciti_keys_to_reprocess.csv` (~2.96M keys, one
   per line, quoted, Athena CTAS export with a `"_col0"` header) without
   loading the whole file into memory.
2. Batches keys (default 500 per batch) and, for each batch, on a virtual
   thread:
   - Checks Elasticsearch (index host default `http://10.30.146.93:9200`,
     index target narrowed per batch from the keys' own dates — see "Known
     risks" below) with a cheap `_count` first using a `terms` query on the
     `key` field. If the count is zero, every key in the batch is missing —
     no need for anything more expensive. Only if the count is non-zero does
     it run a `_search` (needed to find out exactly *which* keys matched).
     No per-key queries, no per-key connections either way.
   - For whichever keys in that batch are **not** already in ES, submits
     them to the backlogger `replayKeys` endpoint as one comma-separated
     request body (matching backlogger's expected raw-string format).
   - Keys already in ES are skipped and not resubmitted.
3. Retries a failed ES check or backlogger call up to 3 times (1s/3s/9s
   backoff) before giving up on that batch and logging it as failed — a
   single bad batch never aborts the whole run.
4. Writes four output files (see below) that double as both an audit trail
   and a **resume ledger**: rerunning against the same `--output-dir` will
   automatically skip keys already handled in a prior run, so an
   interrupted run can simply be restarted.

Concurrency to ES and backlogger is bounded by `--concurrency` (default
100) via a semaphore, regardless of how many virtual threads/batches exist
in total — this keeps the tool from overwhelming either service while
still getting the throughput benefit of not blocking OS threads on I/O.

One `HttpClient` instance is created once and reused for every request for
the whole run (connection pooling/keep-alive) — this is deliberate, to
avoid the very slow "one curl process per key" pattern the earlier shell
scripts (`rssmb/check_keys_parallel.sh` etc.) suffered from.

## Build

```powershell
cd C:\git\data\CITI_Migration\swfa.citi\backlogger-reprocess
mvn clean package
```

Produces the runnable fat jar: `target\backlogger-reprocess-1.0.0.jar`.

## Configuration

| Setting | Env var (required) | CLI override |
|---|---|---|
| Elasticsearch host | `BACKLOGGER_ES_HOST` | `--es-host <url>` |
| ES index name prefix | `BACKLOGGER_ES_INDEX_PREFIX` | `--es-index-prefix <str>` |
| Backlogger `replayKeys` URL | `BACKLOGGER_REPLAY_URL` | `--backlogger-url <url>` |
| UAA OAuth token URL | `BACKLOGGER_TOKEN_URL` | `--token-url <url>` |
| OAuth `client_id` | `BACKLOGGER_CLIENT_ID` | — |
| OAuth `client_secret` | `BACKLOGGER_CLIENT_SECRET` | — |

**ES index name prefix** (e.g. `rmaas-tier2-`) is the literal string
`EsBatchChecker` appends each batch's own `YYYY-MM-*` to (see "Known risks"
below for why it's narrowed per batch) — this makes the tool reusable
against a differently-named index family without a code change, same as
every other host/URL setting.

| Other setting | CLI flag | Default |
|---|---|---|
| Input CSV | `--input <path>` | `swfaciti_keys_to_reprocess.csv` |
| Batch size | `--batch-size <int>` | `500` |
| In-flight concurrency | `--concurrency <int>` | `100` |
| Output directory | `--output-dir <path>` | `./output` |
| Limit (test runs) | `--limit <int>` | unlimited |

### How the backlogger token works

There's no static `BACKLOGGER_TOKEN` anymore. Instead, `TokenProvider`
fetches an OAuth2 access token via `client_credentials` at startup:

```
POST {BACKLOGGER_TOKEN_URL}
content-type: application/x-www-form-urlencoded
grant_type=client_credentials&client_id={BACKLOGGER_CLIENT_ID}&client_secret={BACKLOGGER_CLIENT_SECRET}
```

The token is fetched **once** (fails fast at startup if the credentials or
URL are wrong — before any ES/backlogger batch work starts), cached in
memory, and reused for every request for the whole run. If a backlogger
call ever gets an HTTP 401 (token expired or revoked mid-run — plausible on
a run lasting an hour+), the cached token is invalidated and a fresh one is
fetched automatically on the next retry attempt — no manual re-run needed.

## Running from the command line

```powershell
$env:BACKLOGGER_ES_HOST = "http://10.10.100.10:9200"
$env:BACKLOGGER_ES_INDEX_PREFIX = "rmaas-tier2-"
$env:BACKLOGGER_REPLAY_URL = "https://ea-tier2-backlogger-v2-rest-rmaas.ea.internal.citi.us-east-1.aws.smarsh.cloud/backlogger/replayKeys"
$env:BACKLOGGER_TOKEN_URL = "https://uaa.ea.internal.citi.us-east-1.aws.smarsh.cloud/oauth/token/"
$env:BACKLOGGER_CLIENT_ID = "<your client id>"
$env:BACKLOGGER_CLIENT_SECRET = "<your client secret>"

java -jar target\backlogger-reprocess-1.0.0.jar `
  --input C:\git\data\CITI_Migration\swfa.citi\swfaciti_keys_to_reprocess.csv `
  --batch-size 500 --concurrency 30 `
  --output-dir .\output
```

For a small test run first (recommended — see Testing below):

```powershell
java -jar target\backlogger-reprocess-1.0.0.jar `
  --input ..\swfaciti_keys_to_reprocess.csv `
  --limit 10 --batch-size 10 --concurrency 1 `
  --output-dir .\test-output
```

## Running from IntelliJ

Create a Run/Debug Configuration for `com.smarsh.backlogger.Main`:

- **Program arguments**: the `--input` / `--limit` / `--batch-size` /
  `--concurrency` / `--output-dir` flags above (these are CLI args parsed by
  `CliConfig`, not JVM settings).
- **Environment variables**: all six from the Configuration table above
  (`BACKLOGGER_ES_HOST`, `BACKLOGGER_ES_INDEX_PREFIX`, `BACKLOGGER_REPLAY_URL`,
  `BACKLOGGER_TOKEN_URL`, `BACKLOGGER_CLIENT_ID`, `BACKLOGGER_CLIENT_SECRET`)
  — read via `System.getenv`, not system properties, so they go in the
  Environment variables field, not VM options.
- **VM options**: leave empty for test runs. For a full 3M-key run you can
  add `-Xmx1g` here if needed, but virtual threads need no special flags
  (`--enable-preview` is not required — stable in Java 21).
- **Working directory**: defaults to the module root; use an absolute path
  in `--input` if you don't want to rely on it.

## Output files (in `--output-dir`)

| File | Contents |
|---|---|
| `skipped-already-in-es.csv` | Keys found in ES — not resubmitted |
| `submitted-to-backlogger.csv` | Keys actually sent to backlogger |
| `failed-batches.csv` | `stage,batch_size,error_message,keys` for batches that exhausted all retries (`stage` is `ES_CHECK` or `BACKLOGGER_SUBMIT`) |
| `run-summary.txt` | Final counts + elapsed time (also logged at the end of `run.log`) |
| `run.log` | Every log line printed during the run (config, resume info, retry warnings, periodic progress, final summary) — timestamped, appended across restarts, mirrors the console output so you have a record after the run finishes |

**Progress logging**: at startup the tool does a quick line-count pass over
the input CSV (fast — no parsing) to estimate the total batch count, then
logs a `Progress: X/~Y batches (~Z%) | skipped=.. submitted=.. failed=.. |
elapsed=..s` line roughly every 2% of the run (about 50 lines total for a
full run). The `~` denotes the total/percentage are an estimate — they
don't account for how many keys the resume ledger ends up filtering out.

`skipped-already-in-es.csv` and `submitted-to-backlogger.csv` are read back
in on every startup as the **resume ledger** — any key already listed there
is skipped on the next run without re-checking ES or resubmitting. Keys in
`failed-batches.csv` are *not* in the ledger, so a rerun naturally retries
them from scratch. Because batches complete out of order across virtual
threads, this per-key ledger is what makes resuming after a crash/interrupt
correct — a simple "last row processed" counter would not be.

## Backlogger success detection

The backlogger API returns no structured detail on success — just an HTTP
2xx with a body containing the word "submitted" (observed as `"Submitted"`,
capitalized). The client checks for this case-insensitively. A key showing
up in `submitted-to-backlogger.csv` means the backlogger call was accepted;
actual re-indexing happens later, asynchronously, depending on backlogger's
own queue — this tool does not poll or wait for that to confirm.

## Testing before a full run

1. `--limit 10`, `--batch-size 10`, `--concurrency 1` against a fresh
   `--output-dir` — check `skipped-already-in-es.csv` /
   `submitted-to-backlogger.csv` for a sane matched/missing split.
2. `--limit 1000-2000` with real backlogger submission — confirm entries
   land in `submitted-to-backlogger.csv`, not `failed-batches.csv`.
3. Resume check: run with `--limit 2000`, kill it partway (Ctrl+C), rerun
   with the **same `--output-dir`** and no/larger `--limit` — console should
   print `Resuming: N keys already handled...` and only process the rest.
4. Failure-path check (optional): point `--es-host` at an unreachable host
   for a small `--limit` run — confirm retries log to console, the batch
   lands in `failed-batches.csv`, and the run still completes with a
   summary instead of aborting.
5. Only after 1-4 look correct, run the full file with a conservative
   `--concurrency` (20-50 to start) and watch `run-summary.txt`.

## Known risks / things to keep in mind

- **ES index target is narrowed per batch to the keys' own date, not the
  fully open `{BACKLOGGER_ES_INDEX_PREFIX}*` wildcard.** Real testing showed
  the open wildcard fans every query out across every shard in the
  cluster's entire history, which repeatedly tripped Elasticsearch's parent
  circuit breaker (`Data too large ... 5.5gb ... limit of 5.4gb`) and even
  caused the node to stop accepting new connections for minutes at a time
  under `--concurrency 10` — the query's `size` wasn't the dominant cost,
  shard fan-out was. `EsBatchChecker` derives the index pattern from each
  key's own `YYYY/MM` prefix (keys look like `2017/07/06/.../file.gz`), e.g.
  `rmaas-tier2-2017-07-*` instead of `rmaas-tier2-*` (both the prefix and
  the date are configurable/derived, not hardcoded — see Configuration
  above). A batch spanning multiple months queries multiple narrow patterns
  (comma-joined) rather than ever falling back to the open wildcard, so
  nothing is silently excluded — this is a correctness-preserving
  optimization, not a fixed date range. `size` is also still set to the
  batch's exact length rather than a fixed large number (a key can match at
  most one document, so this is never truncated and is cheaper for ES to
  serve). If you still see `circuit_breaking_exception`/
  `task_cancelled_exception` or connection timeouts after this fix, the
  cluster is likely under capacity for even a narrowed query at that
  `--concurrency` — reduce it further rather than assuming it's a code bug.
- No documented rate limit exists for the backlogger API — start
  `--concurrency` low and increase only if it tolerates it.
- If a batch fails and its keys end up in `failed-batches.csv` after a
  false failure (e.g. a bug in success detection), reprocess by rerunning
  against the same output dir once the underlying issue is fixed — failed
  keys are always retried since they're never added to the resume ledger.

---

# Tool 2: Duplicate source-id checker (`com.smarsh.backlogger.DuplicateSourceIdChecker`)

Standalone, unrelated to the backlogger pipeline. For each sourceId in a
CSV, runs a single ES `_count` query matching
`"X-SMARSH-SOURCE-ID:{sourceId}"` as a phrase inside `text.sys.content`,
plus a wide `startTime` range filter, and classifies the id by how many
docs matched:

| Result | Output file |
|---|---|
| count == 0 | `zero-count-source-ids.csv` |
| count == 1 | `unique-source-ids.csv` |
| count > 1 | `duplicate-source-ids.csv` (`sourceId,count`) |
| query error | `failed-source-ids.csv` (`sourceId,error`) — single attempt, no retry |

Unlike the S3-key `terms` check, a sourceId can't be batched with
others — each `query_string` phrase match is inherently per-id, so this
issues **one `_count` call per row**, one per virtual thread, with overall
concurrency to ES bounded by `--concurrency`. Start conservative (2-5):
this is a heavier full-text query with no per-item date narrowing
available (sourceIds don't carry a date the way S3 keys do).

**Resumable**: on startup, ids already present in
`unique-source-ids.csv`/`duplicate-source-ids.csv`/`zero-count-source-ids.csv`
from a prior run (same `--output-dir`) are loaded and skipped. Failed ids
are *not* in this ledger, so a rerun naturally retries them.

## CLI arguments

| Flag | Default |
|---|---|
| `--input <path>` | `../duplicates_source_ids.csv` |
| `--concurrency <int>` | `3` |
| `--limit <int>` | unlimited |
| `--output-dir <path>` | `output-duplicates` |
| `--es-host <url>` | env `BACKLOGGER_ES_HOST` |
| `--es-index-prefix <str>` | env `BACKLOGGER_ES_INDEX_PREFIX` |

Reuses the same `BACKLOGGER_ES_HOST`/`BACKLOGGER_ES_INDEX_PREFIX` env vars
as Tool 1 (same ES cluster), but needs no backlogger token — this tool
never calls backlogger.

## Running

```powershell
$env:BACKLOGGER_ES_HOST = "http://10.10.100.10:9200"
$env:BACKLOGGER_ES_INDEX_PREFIX = "rmaas-tier2-"

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.backlogger.DuplicateSourceIdChecker `
  --input ..\duplicates_source_ids.csv --limit 20 --concurrency 2 --output-dir .\output-duplicates
```

---

# Tool 3: Athena year key-count (`com.smarsh.athena.Main`)

Standalone, unrelated to ES/backlogger. For each "year bucket" — a
calendar year 2000-2026, or one of three special multi-year buckets
`pre-1970` / `1970-1999` / `post-2026` — runs one Athena query counting
`COUNT(DISTINCT key)` per year, filtered by that bucket's `quadrimester`
value(s):

```sql
SELECT YEAR(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000)) AS year,
    COUNT(DISTINCT key) AS key_count
FROM {table}
WHERE reporting_entity = '{reportingEntity}'
  AND quadrimester IN (...)   -- that bucket's quadrimester value(s)
GROUP BY YEAR(FROM_UNIXTIME(CAST(start_time AS BIGINT) / 1000))
ORDER BY year
```

and appends every `(year, key_count)` row to a local output CSV as each
bucket's query finishes.

- **Full default range** = 27 years (2000-2026) + 3 special buckets = 30
  queries. Use `--years <csv>` to run a specific subset instead, e.g.
  `--years 2023` or `--years 2020,2021,pre-1970`.
- **`ResultConfiguration` is only set if you pass `--s3-output-location`.**
  Athena requires an S3 staging location for every query regardless of
  whether it's the workgroup's default or specified explicitly — if your
  workgroup has none configured, you'll get `"No output location
  provided"` unless you pass one. This tool never reads/manages that S3
  object itself either way, it only calls `GetQueryResults` and writes
  rows locally.
- A single retry (2 attempts, 5s apart) per bucket; a bucket that still
  fails is logged and skipped, the run continues with the rest.
- **Resumable per bucket**: a `<output-csv>.processed-buckets.txt`
  manifest records each bucket label once its query succeeds. On the next
  run (same `--output-csv`), buckets already in that manifest are skipped
  — no Athena call at all. This is tracked separately from the output
  CSV on purpose: the special buckets write rows keyed by the *actual*
  data year found (e.g. `1975`), not the bucket label, and a bucket with
  genuinely zero matching data writes no CSV rows at all but is still
  "done" — so completion can't be inferred from the CSV's own contents.
  Each Athena query is all-or-nothing (no partial results on failure), so
  a failed bucket reruns from scratch next time, never "resumes mid-year."

## AWS credentials

No credential handling in this code — the AWS SDK's default credential
provider chain picks up the standard env vars automatically:
`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN` (for
temporary/STS credentials).

## CLI arguments

| Flag | Required? | Default |
|---|---|---|
| `--database <name>` | **Required** | — |
| `--reporting-entity <value>` | **Required** | — |
| `--table <name>` | optional | `tier2_migration_duplicate_stage2` |
| `--region <str>` | optional | `us-east-1` |
| `--workgroup <str>` | optional | `primary` — **check your actual workgroup name**, see below |
| `--years <csv>` | optional | full range: `2000..2026` + `pre-1970,1970-1999,post-2026` |
| `--output-csv <path>` | optional | `./year-key-counts.csv` |
| `--concurrency <int>` | optional | `3` |
| `--s3-output-location <uri>` | optional (required if your workgroup has no default) | none |
| `--count-field <name>` | optional | `key` — the column `COUNT(DISTINCT ...)` is applied to, e.g. `source_id` to count distinct source ids per year instead |

**On `--count-field`**: the output CSV's second column is named
`{count-field}_count` (e.g. `key_count` or `source_id_count`), so switching
fields between runs needs a different `--output-csv` anyway — don't reuse
the same output file across different `--count-field` values, since the
resume manifest (`<output-csv>.processed-buckets.txt`) is keyed by bucket
label only, not by field, and would incorrectly skip buckets that were
only completed for a *different* field.

**On `--workgroup`**: real testing showed `primary` has no default output
location configured, and the database being queried isn't even in that
workgroup. Use the workgroup name shown in the Athena console (top-right
of the query editor, or **Athena → Workgroups**) for whatever workgroup
you actually use to query this database — that workgroup likely already
has a default output location, letting you skip `--s3-output-location`
entirely.

## Running

```powershell
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN = "..."

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.athena.Main `
  --database <your athena database> `
  --reporting-entity njfa.citi `
  --workgroup <your actual workgroup name> `
  --years 2023 `
  --output-csv test-year-key-counts.csv
```

Drop `--years` to run the full 30-bucket range once a single-year test
looks right.

### AWS SDK version note

The org's Artifactory proxy returns `403 Forbidden` for most AWS SDK v2
versions (not Athena-specific — same happened for `s3` and for AWS SDK
v1). Version **2.21.13** (pinned in `pom.xml`) happened to already be
fully cached locally from an earlier successful resolution. If building on
a machine without that `.m2` cache, either get IT to allowlist
`software.amazon.awssdk` in Artifactory, or copy the `2.21.13` folders
from an existing `.m2` cache.
