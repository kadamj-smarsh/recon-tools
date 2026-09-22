# com.smarsh.sourcelookup

For each `source_id` in a CSV: finds its `key` in Elasticsearch, derives
the Athena `quadrimester` partition from that key's own date, then runs a
narrowed `SELECT COUNT(*)` in Athena to confirm the record's presence
there too — without scanning the whole `tier2_migration_duplicate_stage2`
table (an un-narrowed `source_id`-only query "gets exhausted", per the
original report).

## Pipeline

1. **ES key lookup** (`EsKeyResolver`) — same `query_string` phrase match
   on `text.sys.content` as `DuplicateSourceIdChecker` (a sourceId is only
   ever present in free text, not a discrete keyword field), `_count`
   first then `_search` for `_source: ["key"]` if any match exists. A
   sourceId can resolve to more than one key (known duplicates resolve to
   2, confirmed in testing) — every key found gets its own row downstream.
   A sourceId with **no** ES match is logged to
   `unresolved-source-ids.csv` and the Athena step is skipped for it
   entirely (no point running an un-narrowed, expensive Athena scan with
   no quadrimester to filter by).
2. **Quadrimester derivation** (`Quadrimester.fromKey`) — the key's own
   `YYYY/MM` prefix maps to `T1` (Jan-Apr) / `T2` (May-Aug) / `T3`
   (Sep-Dec), e.g. `2010/06/...` → `2010-T2` (matches the example given
   exactly). A key that doesn't parse as `YYYY/MM/...` is logged to
   `failed-source-ids.csv` with stage `QUADRIMESTER_DERIVE`.
3. **Athena confirmation** (`AthenaCountLookup`) — one narrowed
   `SELECT COUNT(*) FROM {table} WHERE reporting_entity='...' AND
   quadrimester='...' AND source_id='...'` per (sourceId, key) pair,
   reusing `AthenaQueryRunner` (com.smarsh.athena) with its own 3-attempt
   retry.

## Files

| File | Responsibility |
|---|---|
| `SourceIdCsvReader.java` | Reads source ids + resume filter |
| `EsKeyResolver.java` | `_count`-then-`_search` per sourceId, returns matching key(s) |
| `Quadrimester.java` | Derives `YYYY-Tn` from a key's date prefix |
| `AthenaCountLookup.java` | Builds/runs the narrowed `COUNT(*)` query, own retry |
| `LookupOutputWriter.java` | The three output files + resume ledger loader |
| `LookupConfig.java` | CLI parsing |
| `SourceIdAthenaLookup.java` | Entry point / wiring (also the class name — deliberately not called `Main`, to stay unambiguous among the other tools in this jar) |

## Configuration

| Setting | Source | Default |
|---|---|---|
| Input CSV | `--input <path>` | **required** |
| Reporting entity | `--reporting-entity <value>` | **required** |
| Athena database | `--database <name>` | **required** |
| Athena table | `--table <name>` | `tier2_migration_duplicate_stage2` |
| AWS region | `--region <str>` | `us-east-1` |
| Athena workgroup | `--workgroup <str>` | `primary` — check your actual workgroup, same caveat as the other Athena tools |
| Athena S3 output location | `--s3-output-location <uri>` | none (only needed if your workgroup has no default) |
| ES host | `--es-host <url>` or env `BACKLOGGER_ES_HOST` | required via one of the two |
| ES index prefix | `--es-index-prefix <str>` or env `BACKLOGGER_ES_INDEX_PREFIX` | required via one of the two |
| Output directory | `--output-dir <path>` | `output-sourcelookup` |
| Concurrency | `--concurrency <int>` | `5` |
| Limit (test runs) | `--limit <int>` | unlimited |

AWS credentials: the AWS SDK's default credential chain picks up
`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` / `AWS_SESSION_TOKEN`
automatically.

## Running

```powershell
$env:BACKLOGGER_ES_HOST = "http://10.10.100.10:9200"
$env:BACKLOGGER_ES_INDEX_PREFIX = "rmaas-tier2-"
$env:AWS_ACCESS_KEY_ID = "..."
$env:AWS_SECRET_ACCESS_KEY = "..."
$env:AWS_SESSION_TOKEN = "..."

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.sourcelookup.SourceIdAthenaLookup `
  --input source_ids.csv `
  --reporting-entity njfa.citi `
  --database <your athena database> `
  --workgroup <your actual workgroup name> `
  --concurrency 5
```

## Output (`--output-dir`)

| File | Contents |
|---|---|
| `resolved-source-id-keys.csv` | `source_id,key,quadrimester,athena_count` — one row per ES key match successfully confirmed against Athena |
| `unresolved-source-ids.csv` | `source_id` — no ES match found at all, Athena step skipped |
| `failed-source-ids.csv` | `source_id,stage,error` — `ES_LOOKUP`, `QUADRIMESTER_DERIVE`, or `ATHENA_COUNT` failures after retries; never added to the resume ledger, so a rerun retries them |
| `run.log` | Config, progress, retry warnings, final summary |

`resolved-source-id-keys.csv` + `unresolved-source-ids.csv` together form
the resume ledger — rerunning against the same `--output-dir` skips any
sourceId already in either file.

## Verified

Tested against real ES with 2 known duplicate source ids: both correctly
resolved to their ES keys (one resolved to **2** keys, matching the known
duplicate), and `quadrimester` was derived correctly from the real key
found (`2013-T1`) — matches the `2010/06/...` → `2010-T2` example exactly
when checked independently. The Athena step reached AWS correctly (a real
AWS-side error came back, not a local one) but wasn't confirmed
end-to-end — needs real database name, workgroup, and valid AWS session
credentials to verify the final `COUNT(*)` result.
