# Implementation status

## How to read this page

This is the evidence ledger for the checked-in project, not the target design. Architecture and contract documents deliberately distinguish **Current** from **Target** behavior; this page records the gaps between them.

Status terms:

- **Implemented** — corresponding code or configuration is present.
- **Previously exercised** — local artifacts or project history indicate a successful developer run, but no clean-state automated test proves it.
- **Partial** — a meaningful implementation exists but does not satisfy the documented end-to-end contract.
- **Blocked** — a known defect prevents the next verified boundary.
- **Designed** — documented target with no complete implementation claim.
- **Open design** — a required policy or mechanism has not yet been selected.

## Capability matrix

| Capability | Status | Evidence or qualification |
|---|---|---|
| Airflow DAG parsing and scheduling | Previously exercised | Airflow 3 configuration and ingestion DAGs are present |
| SEC-API.io company/ticker extraction to MinIO | Previously exercised | Ingestion DAG and local landing artifacts exist |
| Massive daily grouped OHLC extraction to MinIO | Previously exercised | Ingestion DAG and source-specific object writer exist |
| Alpha Vantage news extraction to MinIO | Previously exercised | Ingestion DAG and source-specific object writer exist |
| MinIO bucket bootstrap | Partial | Compose defines `landing`, `staging`, and `iceberg`; the example endpoint is invalid inside containers |
| Landing-to-bronze processing | Partial | Spark code exists; path, call-signature, catalog, and runtime issues block a clean run |
| Bronze high watermark | Partial | A run-ID watermark design exists, but lookup signatures, identities, field names, and update SQL are inconsistent |
| Bronze-to-silver processing | Partial | Transform code exists but contains multiple runtime and SQL defects |
| Snapshot-based silver incrementality | Partial | Iceberg snapshot tracking code exists; orchestration, read bounds, and checkpoint correctness are incomplete |
| Great Expectations bronze validation | Partial | Persisted bronze artifacts exist; invocation and naming need repair |
| Great Expectations silver validation | Blocked | Suite construction contains rule-definition and API errors |
| dbt gold models | Partial | Project skeleton and some dimensions exist; sources, facts, SQL, profile example, and verified Iceberg materialization are incomplete |
| End-to-end reproducible batch run | Blocked | Infrastructure and cross-layer contract mismatches prevent clean execution |
| Automated replay/late-discovery regression tests | Designed | No complete automated suite or CI workflow is checked in |
| Retention and snapshot expiration | Open design | No safe lifecycle policy or automated cleanup is configured |
| Validation-gated table visibility | Designed | A publication-manifest contract is documented, but no staged candidate or trusted-snapshot resolver is implemented |
| Historical Stooq normalization | Blocked | The uploader exists, but no separate raw-table parser, fixture-backed mapping, or provider-precedence rule is implemented |

## High-watermark assessment

Yes, the project contains a high-watermark approach for incremental batch loading:

- landing to bronze is intended to track the last successfully processed monotonic `run_id`;
- bronze to silver is intended to track the last consumed Iceberg snapshot (target column `last_consumed_snapshot_id`; current code uses `last_snapshot_id`);
- checkpoints are intended to move only after a successful write and validation.

The core technique is appropriate for this architecture. It is not yet reliable in the current checkout because the helper and callers disagree on schemas and identities, and its merge SQL is invalid. A per-run processing ledger is additionally required for failed-run audit, corrections, and runs discovered with IDs below the stored maximum. An older business date with a newly increasing ingestion ID is still eligible under the simple watermark. The detailed contract is in [Incremental loading](incremental-loading.md).

## Blockers to a clean end-to-end run

### P0 — platform boundary

1. **Airflow cannot reliably resolve MinIO.** Airflow services are not attached to the external `lakehouse-network`, while the configured endpoint uses `http://minio:9000`.
2. **One MinIO endpoint is overloaded for host and container clients.** `docker/.env.example` supplies `http://localhost:9000` to Hive and bucket-bootstrap containers, where `localhost` is not MinIO. The host uploader also needs a host-resolvable endpoint in the format expected by the MinIO Python client.
3. **The active Spark image lacks the intended dependencies.** Compose runs `apache/spark:3.5.0`; the custom Spark Dockerfile is not referenced. That Dockerfile also selects a Spark 3.4 Iceberg runtime for a Spark 3.5 base.
4. **Catalog names are inconsistent.** Spark configuration declares `iceberg_catalog`, while processing code refers to `hive_prod`; schema bootstrap is not consistently catalog-qualified.
5. **Batch source code is not mounted into Spark containers.** Compose mounts `docker/spark/spark-apps`, but jobs import code under `scripts/batch`.
6. **Spark Thrift is not configured for the target Iceberg gold layer.** The service uses the stock image, does not mount Spark/Hive/S3 configuration, and its command still references Delta and `s3a://delta`; dbt has no verified Iceberg materialization profile.

### P0 — ingestion-to-bronze contract

7. **Landing keys and lineage metadata do not match bronze discovery.** DAGs write prefixes such as `tickers`, `news_sentiment`, and `EOD_stock_prices`; bronze scans different dataset names and expects `run_id=...` segments the DAGs do not create. Its date regex accepts `YYYY_MM_DD`, not the target `source_date=YYYY-MM-DD`, and current writers do not persist the target run/checksum metadata. Because current keys are date-scoped and use `replace=True`, a rerun can also mutate source evidence after downstream consumption.
8. **Bronze job calls do not match helper signatures.** Required arguments are omitted and one dataset name is misspelled as `comapnies`.
9. **The watermark store is internally inconsistent.** Specific defects include:

   - table creation declares `last_processed_run_id`, while the DataFrame and merge use `last_run_id`;
   - bronze omits the required database argument to `get_watermark`;
   - silver constructs a duplicated qualified lookup identity, while writes store an unqualified table name;
   - an empty lookup indexes `row[0]`, and the merge statement has missing syntax;
   - bronze declares a separate watermark-table constant that the shared helper does not use.

### P1 — transformation and publication

10. **Silver jobs have execution defects.** These include misspelled columns, invalid `from_json` use, incorrect Spark types, malformed Iceberg merge SQL, wrong source/target references, and incomplete checkpoint orchestration.
11. **Ingestion retry paths are defective in current local DAG edits.** The OHLC and news DAGs import `time` from `datetime` but call `time.sleep`; their terminal retry condition is also unreachable.
12. **Airflow providers are not pinned explicitly.** DAG imports depend on HTTP and Amazon provider packages absent from `airflow/requirements.txt`.
13. **Silver validation suites do not build reliably.** Expectation definitions, names, and rule dictionary assumptions are inconsistent.
14. **Validation invocation is inconsistent.** Layer/table argument order and persisted suite names do not align.
15. **Gold modeling does not yet implement the [target dimensional model](gold-data-model.md).** No dbt source/schema YAML or reusable profile example is present; `dim_topic` has no model and an empty seed; facts are empty or skeletal; checked-in news fact names differ from the target bridge names; and several models contain invalid SQL. The company snapshot uses only `cik`, the dimensions omit required natural keys/attributes, SCD closures are missed, and the current incrementing-key macro is not deterministic for distributed builds.
16. **A checkpoint is not yet a reader-visibility boundary.** Jobs write directly to target tables; no write-audit-publish branch, staging table, or published-snapshot registry prevents direct readers from seeing a commit that later fails validation.
17. **Historical OHLC has no conforming transform.** Stooq objects contain many observation dates but the current bronze contract assumes one path-derived source date; no separate raw-table parser, accepted field mapping, or Massive-versus-Stooq precedence is implemented.

## Recommended recovery sequence

Repair one vertical slice before broadening the platform:

1. Split host/internal MinIO endpoints and align Docker networks, Spark/Iceberg dependencies, catalog name, code mounts, and service health checks.
2. Correct ingestion retry behavior and make all landing objects follow one versioned key contract.
3. Repair the run-ID watermark and run ledger, then prove replay-safe landing-to-bronze for `companies`.
4. Repair the companies silver transform, add a validation-gated publication mechanism, and validate snapshot-based bronze-to-silver loading.
5. Build the company gold dimension/snapshot and its dbt tests.
6. Add same-run replay, older-source-date, late-discovered-ID, lineage, and count-reconciliation tests for that vertical slice.
7. Repeat the proven pattern for OHLC, then news and child tables.
8. Add clean-state smoke tests and CI only after the local bootstrap is deterministic.

This order converts the architecture into a testable thin slice and avoids debugging three provider shapes while the shared platform boundary is still unstable.

## Exit criteria for the next milestone

The companies vertical slice is complete when a reviewer can:

1. start the required services from empty local state;
2. ingest one fixed SEC-API.io response under the canonical landing key;
3. produce one source-faithful bronze run with complete lineage;
4. validate and publish the current silver company listing snapshot;
5. build and test the expected gold company model;
6. replay the same run without changing business row counts;
7. process an older source date with a new increasing run ID, then recover a late-discovered lower ID through the target ledger exactly once;
8. trace a gold record back to its silver row, bronze snapshot/run, and landing object.

## Maintenance rule

Update this page in the same change that alters a capability status. Promote a row to **Implemented** only when a repeatable verification command or automated test supports the claim. Do not use planned architecture as evidence of running behavior.

## Related documents

- [Architecture](architecture.md)
- [Gold data model](gold-data-model.md)
- [Incremental loading](incremental-loading.md)
- [Quality and observability](quality-and-observability.md)
- [Operations](operations.md)
- [Documentation index](README.md)
- [Project README](../README.md)
