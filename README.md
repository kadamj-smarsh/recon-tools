# backlogger-reprocess

Java 21 CLI tool to reprocess S3 keys via the backlogger `replayKeys` API,
skipping keys that are already present in Elasticsearch.

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

| Setting | How | Default |
|---|---|---|
| Backlogger auth token | Env var `BACKLOGGER_TOKEN` (required — program exits with a clear error if unset) | none |
| Input CSV | `--input <path>` | `swfaciti_keys_to_reprocess.csv` |
| Batch size | `--batch-size <int>` | `500` |
| In-flight concurrency | `--concurrency <int>` | `100` |
| Output directory | `--output-dir <path>` | `./output` |
| Limit (test runs) | `--limit <int>` | unlimited |
| ES host | `--es-host <url>` | `http://10.30.146.93:9200` |
| Backlogger URL | `--backlogger-url <url>` | ea-tier2-backlogger-v2 `replayKeys` URL |

## Running from the command line

```powershell
$env:BACKLOGGER_TOKEN = "<your real bearer token>"

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
- **Environment variables**: `BACKLOGGER_TOKEN=<your token>` (read via
  `System.getenv`, not a system property — do not put it in VM options).
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
  fully open `rmaas-tier2-*` wildcard.** Real testing showed the open
  wildcard fans every query out across every shard in the cluster's entire
  history, which repeatedly tripped Elasticsearch's parent circuit breaker
  (`Data too large ... 5.5gb ... limit of 5.4gb`) and even caused the node to
  stop accepting new connections for minutes at a time under `--concurrency
  10` — the query's `size` wasn't the dominant cost, shard fan-out was.
  `EsBatchChecker` now derives the index pattern from each key's own
  `YYYY/MM` prefix (keys look like `2017/07/06/.../file.gz`), e.g.
  `rmaas-tier2-2017-07-*` instead of `rmaas-tier2-*`. A batch spanning
  multiple months queries multiple narrow patterns (comma-joined) rather
  than ever falling back to the open wildcard, so nothing is silently
  excluded — this is a correctness-preserving optimization, not a
  hardcoded date range. `size` is also still set to the batch's exact
  length rather than a fixed large number (a key can match at most one
  document, so this is never truncated and is cheaper for ES to serve).
  If you still see `circuit_breaking_exception`/`task_cancelled_exception`
  or connection timeouts after this fix, the cluster is likely under
  capacity for even a narrowed query at that `--concurrency` — reduce it
  further rather than assuming it's a code bug.
- No documented rate limit exists for the backlogger API — start
  `--concurrency` low and increase only if it tolerates it.
- If a batch fails and its keys end up in `failed-batches.csv` after a
  false failure (e.g. a bug in success detection), reprocess by rerunning
  against the same output dir once the underlying issue is fixed — failed
  keys are always retried since they're never added to the resume ledger.
- Reprocessing archived communications data may fall under
  retention/compliance rules (e.g. SEC 17a-4-style obligations); that
  determination is outside this tool's scope and should be validated by
  the relevant compliance function before a full production-scale run.
