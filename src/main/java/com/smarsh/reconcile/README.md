# com.smarsh.reconcile

Reconciles Elasticsearch vs. Athena unique-key counts for a given
`reporting_entity`, drilling from month down to second **only where
counts actually differ**, and writing the actual outstanding (mismatched)
keys to a CSV once a mismatched bucket is small enough to fetch directly.

Part of the `backlogger-reprocess` module/jar (see the project root
README for the other two tools sharing this build) — this tool has no
dependency on the backlogger/ES-key-check pipeline itself, only on the
same ES cluster and the shared `AthenaQueryRunner`.

## Algorithm

For each requested year:

1. **Month level** — one ES `date_histogram` (interval=month) and one
   Athena `GROUP BY DATE_FORMAT(...,'%Y-%m')` query (filtered by all 3 of
   that year's quadrimesters). Compare month-by-month. Months where ES
   and Athena already agree are discarded immediately.
2. For each mismatched month: if ES's count for that month is **< 10000**,
   skip straight to a **leaf key fetch** (step 5) for the whole month.
   Otherwise, drill to **day level** — same comparison, narrowed to that
   month and a single quadrimester.
3. Repeat the same "compare → discard if equal → leaf if ES count < 10000,
   else drill deeper" logic at **hour**, then **minute** level.
4. At **second level** (the deepest — no finer granularity exists), any
   remaining mismatch goes to leaf key fetch regardless of count.
5. **Leaf key fetch**: pull every actual key from ES (`_source: ["key"]`,
   `size: 10000`, paginated via `search_after` if the bucket's ES count is
   still ≥ 10000 even at second-level — a rare edge case, but handled
   correctly rather than silently truncated) and every distinct key from
   Athena (`SELECT DISTINCT key`, no cap — the Athena result paginator
   already returns everything). Diff the two key sets and write whichever
   keys are one-sided to the output CSV.

This means the recursion only ever goes as deep as it needs to per
branch — a month that's already `ES count < 10000` never touches
day/hour/minute/second at all; a month with millions of records but a
handful of genuinely mismatched days only drills into those specific
days, not all 30-31 of them.

Runs **sequentially**, not concurrently — this is a diagnostic tool, not
a bulk job, and real mismatches are expected to be sparse enough that the
natural fan-out stays small. If a given year turns out to have pervasive
mismatches at every level, that's itself a signal worth investigating
before drilling further.

## Quadrimester mapping

`T1` = Jan-Apr, `T2` = May-Aug, `T3` = Sep-Dec (confirmed). The month
level filters on all three (`quadrimester IN (...)`); every level below
month filters on the single quadrimester containing that specific month.

## Files

| File | Responsibility |
|---|---|
| `TimeWindow.java` | Window boundary math for every level (year→month→day→hour→minute→second), ES `gte`/`lte` and Athena `TIMESTAMP` formatting, quadrimester lookup |
| `EsHistogramClient.java` | ES `date_histogram` aggregations per level + leaf key fetch with `search_after` pagination |
| `AthenaLevelQueries.java` | Builds/runs the Athena SQL for each level (reuses `com.smarsh.athena.AthenaQueryRunner` as-is) |
| `Reconciler.java` | The recursive drill-down + diff + CSV writing |
| `ReconcileConfig.java` | CLI parsing |
| `ReconcileStats.java` | Summary counters printed at the end of the run |
| `Main.java` | Entry point / wiring |

## Configuration

| Setting | Source | Default |
|---|---|---|
| Years to reconcile | `--years <csv>` (**required**) | e.g. `--years 2013` or `--years 2013,2023` |
| Reporting entity | `--reporting-entity <value>` (**required**) | — |
| Athena database | `--database <name>` (**required**) | — |
| Athena table | `--table <name>` | `tier2_migration_duplicate_stage2` |
| AWS region | `--region <str>` | `us-east-1` |
| Athena workgroup | `--workgroup <str>` | `primary` — check your actual workgroup, same caveat as the Athena year-key-count tool |
| Athena S3 output location | `--s3-output-location <uri>` | none (needed only if your workgroup has no default) |
| ES host | `--es-host <url>` or env `BACKLOGGER_ES_HOST` | (required via one of the two) |
| ES index prefix | `--es-index-prefix <str>` or env `BACKLOGGER_ES_INDEX_PREFIX` | (required via one of the two) |
| Output CSV | `--output-csv <path>` | `./reconcile-outstanding-keys.csv` |

ES index target per year is `{esIndexPrefix}{year}-*` (e.g.
`rmaas-tier2-2013-*`), narrower than the fully open wildcard for the same
reason as the backlogger tool — reduces shard fan-out.

AWS credentials: the AWS SDK's default credential chain picks up
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
automatically — nothing AWS-specific is handled in this code.

## Running

From the `backlogger-reprocess` module root, after `mvn clean package`:

```powershell
$env:BACKLOGGER_ES_HOST = "http://10.10.100.10:9200"
$env:BACKLOGGER_ES_INDEX_PREFIX = "rmaas-tier2-"
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN = "..."

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.reconcile.Main `
  --years 2013 `
  --reporting-entity <reporting-entity> `
  --database <your athena database> `
  --workgroup <your actual workgroup name> `
  --output-csv reconcile-2013.csv
```

Recommend testing with a single year first, ideally one you already
suspect has a known discrepancy, before running multiple years at once —
this walks a real recursion tree against two live systems, so it's worth
watching the console output for the first run to see how deep/wide it
actually drills before trusting a bigger batch.

## Output CSV

`level,window_start,window_end_exclusive,source,key` — one row per
outstanding key, where `level` is whichever granularity the leaf compare
happened at (`month`/`day`/`hour`/`minute`/`second`), `window_start`/
`window_end_exclusive` are the exact bounds of that leaf window (Athena
timestamp format, half-open), and `source` is `ES_ONLY` or `ATHENA_ONLY`.

Console output also prints a per-level trail as it drills (`Month
2013-01 differs: ES=... Athena=...`, `Day 15 differs: ...`, etc.) and a
final summary of how many buckets mismatched at each level and how many
outstanding keys were found on each side.

## Known risks / things to verify before trusting a full run

- **Both ES and Athena calls retry on transient failure** (3 attempts,
  2s/5s backoff) — added after a real `HTTP connect timed out` aborted an
  entire year's reconciliation on the first real run. A whole year is
  still abandoned (logged, other years continue) if all 3 attempts fail
  for a single query.
- **Not yet tested against real ES/Athena data** — the query construction
  and window-boundary math have been verified independently (unit-style
  checks reproducing the exact sample queries/timestamps given), but the
  end-to-end drill-down logic against live systems has not.
- **Combinatorial fan-out risk**: if mismatches turn out to be widespread
  rather than sparse (e.g. every day of a mismatched month also
  mismatches, every hour of every mismatched day, etc.), this could issue
  a large number of Athena queries in one run. Athena has account-level
  query concurrency limits; since this tool runs sequentially it won't
  trip a concurrency limit, but a pathological case could still take a
  long time. Worth watching the console trail on first use.
- **`ResultConfiguration`** is only set if `--s3-output-location` is
  passed — same requirement as the Athena year-key-count tool: if your
  workgroup has no default output location, every Athena call in this
  tool will fail with `"No output location provided"` until you supply
  one or point `--workgroup` at one that has it configured.
- **No resumability** — unlike the backlogger/duplicate-checker tools,
  this one has no ledger. If interrupted partway through a year, rerun
  the whole `--years` value again; the output CSV will just get more
  rows appended (potential duplicate rows across runs if the same
  mismatch is found twice — dedupe downstream if that matters for your
  analysis).
