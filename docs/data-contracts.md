# Data contracts

> **Status:** This is the target contract. The checked-in implementation has known deviations documented in [Implementation status](implementation-status.md).

This document owns source, landing, bronze, and silver shapes, grains, keys, lineage, partitioning, and the cross-layer boundary inventory. Detailed gold dimensional semantics belong to [Gold data model](gold-data-model.md); read/write selection, watermarks, and rerun semantics belong to [Incremental loading](incremental-loading.md).

## Contract principles

1. Landing is source-faithful: ingestion does not rename fields or discard provider metadata.
2. Every downstream row is traceable to a landing object and bronze run.
3. Every table has one declared grain and replay-safe business key.
4. Operational timestamps are never business keys.
5. Publication follows validation.
6. Ingestion owns landing, Spark owns bronze/silver, and dbt owns gold.

## Correctness invariants

| Invariant | Required behavior |
|---|---|
| Run identity | One stable `run_id` follows a dataset run from landing into silver lineage |
| Source identity | `source_file` identifies the exact object; a checksum detects changed content under one key |
| Event time | Business dates/timestamps come from source data or the logical interval |
| Idempotency | Reprocessing a published run does not increase business-key counts |
| Uniqueness | Each target enforces its documented grain |
| Traceability | Every silver/fact row can be traced to a bronze run and landing object |
| Publication | A layer becomes trusted only after candidate writes validate and a manifest names the published snapshot set |
| Checkpoint safety | Failed or partial work cannot move a checkpoint beyond unprocessed input |

## Sources and canonical lineage

| Dataset | Source/cadence | Source envelope | Bronze | Silver | Gold consumers |
|---|---|---|---|---|---|
| `companies` | SEC-API.io; monthly full snapshot | Root JSON array | `hive_prod.bronze.companies` | `hive_prod.silver.companies` | company snapshot, `dim_company_exchange` |
| `ohlcs` | Massive API; daily | Object with `results[]` | `hive_prod.bronze.ohlcs` | `hive_prod.silver.ohlcs` | `fact_ohlc_eod_snapshot` |
| `news_sentiments` | Alpha Vantage; daily | Object with `feed[]` | `hive_prod.bronze.news_sentiments` | parent, ticker, and topic tables | `dim_news`, `fact_news_ticker_bridge`, `fact_news_topic_bridge` |
| historical `ohlcs` | Stooq/manual files | One multi-day text file per ticker plus manifest | `hive_prod.bronze.ohlc_history_files` | `hive_prod.silver.ohlcs` after parsing and precedence rules | `fact_ohlc_eod_snapshot` |

Current Airflow keys use `tickers`, `EOD_stock_prices`, and `news_sentiment`; consumers use different names. The target uses the canonical dataset stem at every layer.

## Naming and time conventions

- Canonical datasets, tables, and transformed columns use lowercase `snake_case`.
- Provider names such as `T` and `isDelisted` remain unchanged only inside `raw_payload`.
- Operational and event timestamps are UTC and end in `_ts`.
- Calendar values use `date` and render as `YYYY-MM-DD` in paths.
- Paths and object keys are case-sensitive.
- Grain and business keys are declared before selecting append, overwrite, or merge.

### Canonical terminology

| Term | Canonical value or meaning |
|---|---|
| Dataset identifiers | `companies`, `ohlcs`, `news_sentiments`, and ingestion dataset `ohlcs_history`, which conforms into silver `ohlcs` under an independent checkpoint |
| `run_id` | Monotonic, sortable UTC identity for one logical ingestion run; every task retry retains it |
| `source_date` | Provider business date or logical source interval; it may be older than the ingestion date |
| `landing_object_key` | Bucket-relative MinIO key written by ingestion |
| `source_file` | Full S3A URI read by Spark and retained as row-level lineage |
| `last_processed_run_id` | Last validated landing run completely written to one bronze target |
| `last_consumed_snapshot_id` | Last bronze input snapshot completely consumed by one silver publication group |
| `source_snapshot_id` / `output_snapshot_id` | Input and output Iceberg snapshot identities recorded for reconciliation |

Use `source_file` in persisted lineage and logs. Use `landing_object_key` when referring specifically to the bucket-relative object key; do not introduce `source_object` as a third name.

## Current ingestion schedule and object keys

This table records checked-in producer behavior. It is intentionally separate from the target canonical layout below.

| Dataset | Producer | Schedule | Logical/source date behavior | Current landing key |
|---|---|---|---|---|
| `companies` | Airflow DAG `py-el-api-tickers` | `@monthly` | Airflow `ds` (`YYYY-MM-DD`) | `tickers/nasdaq/<ds>.json` |
| `ohlcs` | Airflow DAG `py-el-api-eod-ohlc-prices` | `30 0 * * *` UTC | Airflow `ds`; Saturdays and Sundays are skipped, but market holidays are not modeled | `EOD_stock_prices/incremental/market=us/<ds>.json` |
| `news_sentiments` | Airflow DAG `py-el-api-news-sentiment` | `30 0 * * *` UTC | Airflow `ds_nodash` bounds the provider query day | `news_sentiment/<ds_nodash>.json` |
| historical `ohlcs` | Host script `scripts/batch/EL_his_eod_prices.py` | Manual | Host ingestion date; source files retain their own observations | `EOD_stock_prices/historical/market=us/exchange=nasdaq/ingested_date=<date>/<file>` plus `_manifest.json` |

Airflow schedules use UTC-aware start dates for the daily DAGs. If the platform timezone configuration changes, source-date semantics must be revalidated rather than inferred from the displayed UI time.

## Source -> landing

### Canonical layout

```text
s3a://landing/<dataset>/run_id=<sortable-utc-run-id>/source_date=<YYYY-MM-DD>/payload.json
```

```text
s3a://landing/companies/run_id=20260324T000000Z/source_date=2026-03-24/payload.json
s3a://landing/ohlcs/run_id=20260324T003000Z/source_date=2026-03-24/payload.json
s3a://landing/news_sentiments/run_id=20260324T003000Z/source_date=2026-03-24/payload.json
s3a://landing/ohlcs_history/run_id=20260324T010000Z/ingested_date=2026-03-24/exchange=nasdaq/<ticker-file>
```

The current DAGs do not write this layout. Existing objects need a migration or a documented legacy-path reader.

### Payloads

| Dataset | Stored object | Records | Source count |
|---|---|---|---|
| Companies | Complete JSON array | root `$` | array length |
| OHLC | Complete response object | `$.results` | `queryCount` / `resultsCount` |
| News | Complete response object | `$.feed` | `items` |
| Historical OHLC | Original ticker files and `_manifest.json` | file rows | manifest `num_total_files` |

A landing object is accepted only when its source read succeeds, its expected envelope exists, MinIO acknowledges the write, and run metadata includes object key, source/logical date, count, size, and checksum.

A run-scoped key is mutable only while that logical run is still being retried and has not been consumed downstream. After its first bronze write, the object and checksum are immutable. Any later source correction receives a new `run_id`; changed content under a consumed key is a contract violation.

## Landing -> bronze

Bronze retains one raw row per source object and adds operational metadata without changing source semantics.

### Row schema

| Column | Spark type | Required | Meaning |
|---|---|---:|---|
| `raw_payload` | `string` | yes | Exact object contents |
| `source_file` | `string` | yes | Full S3A URI read by Spark |
| `source_checksum` | `string` | yes | SHA-256 of the exact landing object contents |
| `source_date` | `date` | yes | Business/source date parsed from the path |
| `run_id` | `string` | yes | Run identity parsed from the path |
| `data_source_type` | `string` | yes | `sec_api`, `massive_api`, `alpha_vantage`, or `stooq` |
| `bronze_ingestion_ts` | `timestamp` | yes | UTC timestamp of the bronze write |

Current bronze code writes `source_type`, while downstream code expects `data_source_type`; the target name is `data_source_type`. It also extracts only underscore-formatted dates (`YYYY_MM_DD`), which cannot parse the target `source_date=YYYY-MM-DD` path and must be repaired with the path migration.

### Historical OHLC bronze schema

A Stooq file contains many observation dates, so its landing ingestion date cannot satisfy the daily API `source_date` contract. Historical files use a separate raw table with one row per file:

| Column | Required | Meaning |
|---|---:|---|
| `raw_payload` | yes | Exact ticker-file contents |
| `source_file` / `source_checksum` | yes | Immutable file lineage |
| `run_id` | yes | Historical import batch identity |
| `ingested_date` | yes | Date the file was written to landing; not an OHLC event date |
| `exchange` | yes | Exchange declared by the import batch |
| `data_source_type` | yes | `stooq` |
| `bronze_ingestion_ts` | yes | UTC bronze-write timestamp |

The manifest is stored and reconciled as the batch control object. Silver derives `source_date` and `event_ts` from each parsed observation row, never from `ingested_date`.

### Tables and partitions

| Dataset | Target | Partition |
|---|---|---|
| Companies | `hive_prod.bronze.companies` | Month derived from `source_date` |
| OHLC | `hive_prod.bronze.ohlcs` | `source_date` |
| Historical OHLC files | `hive_prod.bronze.ohlc_history_files` | `ingested_date` |
| News | `hive_prod.bronze.news_sentiments` | `source_date` |

Bronze appends unseen raw runs. A controlled correction may replace only the complete affected partition, never unrelated partitions or the table.

## Bronze -> silver

Silver parses provider envelopes into typed domain records while retaining lineage.

### Record extraction

| Dataset | Records extracted from `raw_payload` |
|---|---|
| Companies | root `$` |
| OHLC | `$.results` |
| News | `$.feed` |
| Historical OHLC | Provider-specific delimited rows; parser contract must be fixed from a representative Stooq fixture before implementation |

Current silver code assumes all payloads are root arrays; only companies match that assumption.

### Required lineage

Every silver output retains:

```text
source_file
source_checksum
source_date
data_source_type
bronze_run_id
bronze_ingestion_ts
silver_ingestion_ts
```

### Grains and keys

| Table | Grain | Replay-safe key | Partition |
|---|---|---|---|
| `companies` | One company/security listing in the published monthly master | `(exchange, ticker, cik)` | None |
| `ohlcs` | One ticker end-of-day bar | `(ticker, event_ts)` | `source_date` |
| `news_sentiment` | One normalized article | `news_id` | `source_date` |
| `news_tickers_sentiment` | One article/ticker relationship | `(news_id, ticker)` | `source_date` |
| `news_topics_sentiment` | One article/topic relationship | `(news_id, topic)` | `source_date` |

### Transformations

Companies:

- reject missing identity fields;
- deduplicate by `(exchange, ticker, cik)`;
- convert camelCase fields to snake_case;
- split location into state and country.

OHLC:

- rename `T` to `ticker`;
- map `v`, `vw`, `o`, `c`, `h`, `l`, and `n` to typed columns;
- convert epoch-millisecond `t` to UTC `event_ts`;
- reject missing ticker/event timestamp;
- deduplicate by `(ticker, event_ts)`.

Historical OHLC:

- parse each ticker file using an explicit, fixture-backed Stooq column mapping;
- derive the observation date/timestamp from each source row;
- map prices and volume into the same canonical OHLC columns and business key;
- retain the source-file checksum on every emitted row;
- apply the accepted Stooq-versus-Massive precedence rule before merging.

No historical parser or accepted column/precedence mapping is checked in yet. Until both exist, historical files stop at landing and must not be represented as conforming silver OHLC data.

News:

- remove URL query parameters and lowercase the normalized URL;
- derive `news_id = sha256(url_norm)`;
- parse `time_published` as UTC;
- retain topic/ticker arrays on the parent;
- synchronize child associations so removed relationships do not remain stale.

## Silver -> gold

dbt is the only gold writer. A source YAML must map logical silver sources to validated Iceberg tables.

This is the boundary inventory. Detailed gold attributes, relationships, cardinalities, SCD2 behavior, and join rules are canonical in [Gold data model](gold-data-model.md).

| Gold object | Input | One-line purpose | Detailed contract |
|---|---|---|---|
| `hive_prod.snapshots.snap_dim_company_exchange` | `silver.companies` | Internal listing SCD2 state | [Snapshot](gold-data-model.md#snap_dim_company_exchange) |
| `dim_company_exchange` | Company snapshot | Versioned company/security listing dimension | [Dimension](gold-data-model.md#dim_company_exchange) |
| `dim_news` | `silver.news_sentiment` | Normalized article dimension | [Dimension](gold-data-model.md#dim_news) |
| `dim_topic` | dbt seed | Canonical news-topic reference dimension | [Dimension](gold-data-model.md#dim_topic) |
| `dim_date` | Generated calendar | Conformed calendar and exchange-day flags | [Dimension](gold-data-model.md#dim_date) |
| `dim_time` | Generated series | Conformed minute and market-session attributes | [Dimension](gold-data-model.md#dim_time) |
| `fact_ohlc_eod_snapshot` | OHLC plus dimensions | Listing/date market observation | [Fact](gold-data-model.md#fact_ohlc_eod_snapshot) |
| `fact_news_ticker_bridge` | News-ticker plus dimensions | Measured article/listing association | [Bridge](gold-data-model.md#fact_news_ticker_bridge) |
| `fact_news_topic_bridge` | News-topic plus dimensions | Measured article/topic association | [Bridge](gold-data-model.md#fact_news_topic_bridge) |

The company snapshot key must match listing grain. Current dbt uses only `cik`, although one CIK can have multiple listed securities. A closure updates `dbt_valid_to` on an existing version, so filtering only for newer `dbt_valid_from` values cannot maintain the gold dimension correctly.

## Schema evolution

| Layer | Policy |
|---|---|
| Landing | Retain the complete provider response |
| Bronze | Keep `raw_payload` stable; add backward-compatible metadata only |
| Silver | Parse explicitly; fail the bounded batch and retain its source/diagnostics instead of silently emitting null-heavy rows. A quarantine store is not yet designed |
| Gold | Treat removals, type changes, grain changes, and key changes as versioned migrations |

Additive provider fields can land immediately. They become queryable only after silver schemas, validation suites, and downstream dbt models change together.

## Contract change checklist

1. Update this document and any relevant decision record.
2. Change producer and consumer together.
3. Add or update representative fixtures.
4. Test migration and replay behavior.
5. Update Great Expectations and dbt tests.
6. Document backfill behavior.

## Related documents

- [Architecture](architecture.md)
- [Gold data model](gold-data-model.md)
- [Incremental loading](incremental-loading.md)
- [Implementation status](implementation-status.md)
- [Documentation index](README.md)
- [Project README](../README.md)
