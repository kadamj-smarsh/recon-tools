# com.smarsh.migration

Migrates deduplicated data out of `tier2_migration_duplicate_stage2` per
quadrimester, working around Athena query exhaustion on a full
quadrimester's dedup by splitting it into 4 smaller per-month UNLOADs,
verifying no `source_id`s were lost/gained, then UNLOADing the verified
result to a permanent destination.

## Why this exists

A single `ROW_NUMBER() OVER (PARTITION BY source_id ...)` across a whole
4-month quadrimester's worth of `tier2_migration_duplicate_stage2` data
"gets exhausted" (Athena resource limits). Splitting the dedup into 4
independent per-month UNLOADs keeps each one small enough to succeed
reliably.

## Pipeline (per quadrimester)

1. **4 monthly dedup UNLOADs** (bounded concurrency via `--concurrency`,
   default 4) — each keeps only the latest row per `source_id`
   (`ROW_NUMBER() ... ORDER BY time_stamp DESC, rn=1`) for that specific
   month, tagged with `year`/`quadrimester`/`month` literal columns, and
   written to **one persistent, per-reporting-entity temp table**
   (`tier2_migration_{entity}_monthly_stage2_data`, `.` in entity replaced
   with `_`) — not a new table per quadrimester. If any of the 4 fails
   after retries, the quadrimester is aborted before the remaining steps.
2. **`MSCK REPAIR TABLE`** discovers the newly-written
   `year=.../quadrimester=.../month=...` partitions.
3. **Verification**: `COUNT(*)`/`COUNT(DISTINCT source_id)` on the temp
   table (filtered by `quadrimester`) compared against the same on the
   original `tier2_migration_duplicate_stage2`. **Pass = the two
   `distinct_sources` counts match.** A mismatch is logged prominently,
   written to `mismatched-quadrimesters.csv`, and the quadrimester is
   **not** marked complete (retried on a future run) — but the overall
   tool run continues to the next quadrimester rather than stopping.
4. **On pass**: a final UNLOAD (no partitioning this time, flat output)
   to the permanent destination:
   `s3://{bucket}/eventlog_recon/tier2/migration/coc/reporting_entity={entity}/MIGRATED/{year}-T{n}/`

**Quadrimesters are always processed sequentially** — only the 4 monthly
UNLOADs *within* one quadrimester run with bounded concurrency. Athena has
account-level query concurrency limits, and this is a stateful multi-step
pipeline per quadrimester.

## Why one persistent table per entity, not one per quadrimester

`month` values (`'01'..'12'`) repeat every year — a `month`-only
partition would let one quadrimester's leftover data collide with a later
run's, and the AWS token in use likely lacks S3 delete permission to
clean that up. Instead: **one long-lived table per entity**
(`CREATE EXTERNAL TABLE IF NOT EXISTS`, never dropped), partitioned by
**`year`, `quadrimester`, `month`** (three levels), giving a stable S3
layout of `.../year=YYYY/quadrimester=YYYY-Tn/month=MM/...` under one
fixed per-entity temp path. Uniqueness comes from the partition columns,
not from a unique table/path per run — so reruns/retries are naturally
safe without needing to delete anything.

## Files

| File | Responsibility |
|---|---|
| `QuadrimesterMonths.java` | Pure calendar utility: year+quadrimester → the 4 constituent months' date boundaries; `QuadKey`/`MonthWindow` records |
| `MigrationSql.java` | All SQL/DDL string building (create-table, per-month UNLOAD, MSCK REPAIR, verification COUNTs, final UNLOAD) + table/path naming |
| `MigrationConfig.java` | CLI parsing |
| `MigrationLedger.java` | Resume ledger (`processed-quadrimesters.txt`) |
| `MismatchReporter.java` | Durable `mismatched-quadrimesters.csv` writer |
| `QuadrimesterProcessor.java` | The per-quadrimester orchestration (steps 1-4 above) |
| `QuadrimesterMigrator.java` | Entry point — sequential loop over pending quadrimesters |

Reuses `com.smarsh.athena.AthenaQueryRunner` (DDL/UNLOAD work unmodified
through the same `StartQueryExecution`/`GetQueryExecution`/
`GetQueryResults` flow already used for `SELECT`), and
`com.smarsh.backlogger.Logger`/`RetryExecutor` for logging/retry — no new
Maven dependency needed.

## Configuration

| Flag | Required? | Default |
|---|---|---|
| `--reporting-entity <value>` | **Required** | — |
| `--years <csv>` | **Required** | e.g. `"2024,2025"` |
| `--database <name>` | **Required** | Athena database |
| `--quadrimesters <csv>` | optional | narrows to specific labels e.g. `"2025-T3"`, must be a subset of what `--years` implies — use this for safe first-time testing |
| `--stage2-table <name>` | optional | `tier2_migration_duplicate_stage2` |
| `--bucket <name>` | optional | `eventlog-rmaas-aws-us-east-1-citigroup-tech-production` |
| `--region <str>` | optional | `us-east-1` |
| `--workgroup <str>` | optional | `primary` — check your actual workgroup, same caveat as the other Athena tools |
| `--s3-output-location <uri>` | optional | none (only needed if your workgroup has no default) |
| `--output-dir <path>` | optional | `migration-output` |
| `--concurrency <int>` | optional | `4` (bounds only the 4 monthly UNLOADs per quadrimester) |

AWS credentials: the AWS SDK's default credential chain picks up
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`.

## Running

```powershell
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN = "..."

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.migration.QuadrimesterMigrator `
  --reporting-entity swfa.citi `
  --years 2025 `
  --database <your athena database> `
  --workgroup <your actual workgroup name> `
  --quadrimesters 2025-T3
```

Drop `--quadrimesters` to process every quadrimester of every requested
year, once a single-quadrimester test looks right.

## Output (`--output-dir`)

| File | Contents |
|---|---|
| `run.log` | Config, progress, retry warnings, final summary |
| `processed-quadrimesters.txt` | Resume ledger — one completed `"YYYY-Tn"` label per line |
| `mismatched-quadrimesters.csv` | `quadrimester,reporting_entity,temp_distinct_sources,temp_total_rows,stage2_distinct_sources,delta,timestamp` — durable record of verification failures |

## Verified so far

- Build succeeds; all required-arg fail-fast checks (`--reporting-entity`,
  `--years`, `--database`) trip correctly.
- `--quadrimesters` subset validation correctly rejects a label not
  implied by `--years`.
- `QuadrimesterMonths.monthsOf` reproduces the correct 4-month boundaries
  for `2025-T3` (Sep 1 through Jan 1 2026).
- `MigrationSql`'s generated `monthlyUnload` SQL for September 2025
  matches the original sample query's structure/date-range exactly, with
  the added `year`/`quadrimester`/`month` columns and 3-column
  `partitioned_by`. All naming helpers (`sanitizedEntity`, `tempTableName`,
  `tempPath`, `finalPath`) and the other 4 SQL builders verified directly
  against the design.

## Not yet tested (needs real AWS access)

This tool performs real DDL (`CREATE TABLE`, `MSCK REPAIR`) and real data
`UNLOAD` — no read-only dry-run mode exists. Given the earlier tools in
this jar only ever needed Athena `SELECT`, **confirm the AWS role/token
actually has `CREATE TABLE`/`MSCK REPAIR TABLE`/`UNLOAD` permissions**
(and S3 `PutObject` on both the temp and final prefixes) before a first
real run. Recommended first test: `--years <one year> --quadrimesters
<that one label>` so exactly one quadrimester (4 UNLOADs) executes, then
manually cross-check the temp table's partition counts and the
verification COUNT queries directly in the Athena console before trusting
the tool's own pass/fail decision on a wider run.

Also worth a quick sanity check: the dedup's "keep latest" logic depends
on `ORDER BY time_stamp DESC` producing correct chronological order —
since `time_stamp` is typed `string` in the temp table, this only works
correctly if the real values are a fixed-width, lexicographically-sortable
format (e.g. ISO-8601).
