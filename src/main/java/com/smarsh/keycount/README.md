# com.smarsh.keycount

For each key in a CSV, counts how many times it appears in Elasticsearch
and writes a `key,count` report. Same idea as
`com.smarsh.backlogger.DuplicateSourceIdChecker`, but for the `key` field
instead of source ids — and much faster, because `key` is an exact-match
field, not free text.

## Why this is faster than the source-id checker

`DuplicateSourceIdChecker` has to run one `query_string` phrase-match
query **per source id**, because the id is buried inside free-text
`text.sys.content`. `key` is an exact `terms`-filterable field, so this
tool checks a whole **batch** (default 500 keys) in one `_search` call —
one query per 500 keys instead of one query per key.

## Why this isn't a terms aggregation (a real finding, not a design choice)

The obvious "fast path" would be a single `terms` aggregation per batch
returning every key's `doc_count` directly. Real testing against this
cluster showed that fails:

```
"reason": "Can't load fielddata on [key] because fielddata is
unsupported on fields of type [keyword]. Use doc values instead."
```

This `key` field has **doc_values disabled** in its mapping — a term
aggregation requires doc_values, so aggregating on it is off the table
here regardless of query shape. Instead, this tool fetches the actual
matching hits (`_source: ["key"]`, exact `terms` filter) and **tallies
counts client-side** — hit retrieval only needs the inverted index, not
doc_values, so it works fine. Pages via `search_after` sorted by `"_doc"`
(Lucene's native order) if a batch has enough duplicate hits to fill one
page (10000) — `"_doc"` sort is also doc_values-free, unlike sorting by
`key` itself would be.

## Files

| File | Responsibility |
|---|---|
| `KeyCsvReader.java` | Streaming CSV reader + batching + resume filter |
| `EsKeyCountBatchChecker.java` | One or more `_search` calls per batch, hits tallied into key→count, paginated if needed |
| `OutputWriter.java` | `key-counts.csv` (doubles as resume ledger) + `failed-batches.csv` |
| `KeyCountConfig.java` | CLI parsing |
| `Main.java` | Entry point / wiring |

## Configuration

Reuses the same ES env vars as the backlogger/duplicate-checker tools:

| Setting | Source | Default |
|---|---|---|
| ES host | `--es-host <url>` or env `BACKLOGGER_ES_HOST` | required via one of the two |
| ES index prefix | `--es-index-prefix <str>` or env `BACKLOGGER_ES_INDEX_PREFIX` | required via one of the two |
| Input CSV | `--input <path>` | **required** |
| Batch size | `--batch-size <int>` | `500` |
| Concurrency | `--concurrency <int>` | `10` |
| Output directory | `--output-dir <path>` | `output-keycount` |
| Limit (test runs) | `--limit <int>` | unlimited |

ES index target per batch is narrowed from each key's own `YYYY/MM` date
prefix (same optimization as `EsBatchChecker`), falling back to the fully
open `{prefix}*` wildcard for any key that doesn't parse as
`YYYY/MM/...` — a generic key list isn't guaranteed to follow the S3-key
date format the way the S3-key reprocess input file does.

## Running

```powershell
$env:BACKLOGGER_ES_HOST = "http://10.10.100.10:9200"
$env:BACKLOGGER_ES_INDEX_PREFIX = "rmaas-tier2-"

java -cp target\backlogger-reprocess-1.0.0.jar com.smarsh.keycount.Main `
  --input keys_to_check.csv --batch-size 500 --concurrency 10 --output-dir output-keycount
```

## Output (`--output-dir`)

| File | Contents |
|---|---|
| `key-counts.csv` | `key,count` for every key checked (0 if never found). Doubles as the resume ledger — rerunning against the same `--output-dir` skips keys already counted. |
| `failed-batches.csv` | `batch_size,error_message,keys` for batches that exhausted all retries — never added to the ledger, so a rerun retries them |
| `run.log` | Config, progress, retry warnings, final summary — persisted alongside console output |

## Verified

Tested against the real ES cluster with 5 known keys — all returned
`count=1` correctly (no false duplicates), and a rerun against the same
`--output-dir` correctly recognized all 5 as already counted and skipped
them without any ES calls. Not yet tested against a batch containing a
genuine duplicate key or one large enough to trigger `search_after`
pagination — worth a targeted test with known-duplicate keys before a
full run, if you have any on hand.
