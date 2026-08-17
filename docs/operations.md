# Operations

> **Status:** Infrastructure and Airflow ingestion have been exercised locally, but the checked-in project is not yet reproducibly runnable end to end. Review [Implementation status](implementation-status.md) before execution.

All commands assume the repository root as the working directory unless a command explicitly changes it.

## Prerequisites

- Docker Engine or Docker Desktop with Compose v2
- GNU Make, optionally
- Python 3.10 or newer for host scripts
- At least 4 GB RAM and 10 GB free disk for Airflow alone; Spark plus Airflow requires more
- SEC-API.io, Massive, and Alpha Vantage credentials for their respective DAGs

```bash
docker version
docker compose version
python3 --version
```

## Environment configuration

Create ignored local environment files only if they are missing:

```bash
test -f docker/.env || cp docker/.env.example docker/.env
test -f airflow/.env || cp airflow/.env.example airflow/.env
```

Review them without committing credentials:

```bash
${EDITOR:-vi} docker/.env
${EDITOR:-vi} airflow/.env
```

Important lakehouse values:

- `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`, and the MinIO endpoint
- `META_DB_*`
- `HIVE_METASTORE_URI`
- Spark master, worker, and exposed-port settings
- `HISTORICAL_DATA_DIR`

The checked-in configuration overloads `MINIO_ENDPOINT` for two incompatible contexts:

- containers need an internal URL such as `http://minio:9000`;
- the host-side historical uploader needs a host-resolvable `host:port` value in the form accepted by the MinIO Python client.

`docker/.env.example` currently uses `http://localhost:9000`, which is invalid inside the Hive and bucket-bootstrap containers. Split the setting into internal and host-client variables as part of the P0 platform repair before treating startup as reproducible.

Important Airflow values:

- `AIRFLOW_UID`, normally `id -u` on Linux
- Airflow build settings; `AIRFLOW_IMAGE_NAME` is currently unused because Compose selects `build: .` and comments out `image:`
- source API keys used when provisioning Airflow connections

API-key variables are not automatically converted into Airflow connections by the current Compose definition.

## Shared Docker network

The lakehouse Compose file requires an external network:

```bash
docker network inspect lakehouse-network >/dev/null 2>&1 \
  || docker network create --driver bridge lakehouse-network
```

The WSL-oriented Make target is:

```bash
make -C docker create-network
```

Current blocker: Airflow services are not attached to this network, while the MinIO Airflow connection uses `http://minio:9000`. Fix the Compose topology before expecting ingestion containers to resolve MinIO reliably. The canonical blocker ledger is [Implementation status](implementation-status.md).

## Airflow connections

The DAGs depend on these IDs exactly.

### `minio`

| Field | Value |
|---|---|
| Type | Amazon Web Services |
| Access key | `MINIO_ACCESS_KEY` |
| Secret key | `MINIO_SECRET_KEY` |
| Extra | `{"endpoint_url": "http://minio:9000"}` |

### `sec_api`

| Field | Value |
|---|---|
| Type | HTTP |
| Host | `api.sec-api.io` |
| Schema | `https` |
| Extra | `{"Authorization": "YOUR_SEC_API_KEY"}` |

Current code passes `conn.extra_dejson` directly as request headers. The authorization value must therefore be at the top level. The old screenshot showing a nested `headers` object does not match current code.

### `massive_api`

| Field | Value |
|---|---|
| Type | HTTP |
| Host | Massive hostname serving grouped aggregates |
| Schema | `https` |
| Extra | Provider-required HTTP authorization header object |

The DAG requests `/v2/aggs/grouped/locale/us/market/stocks/{date}` and passes Extra directly as headers.

### `alphavantage_api`

| Field | Value |
|---|---|
| Type | HTTP |
| Host | `www.alphavantage.co` |
| Schema | `https` |
| Extra | `{"api_key": "YOUR_ALPHA_VANTAGE_KEY"}` |

The news task sends this as the `apikey` query parameter.

For reproducibility, connection provisioning should eventually use environment variables, secrets, or checked-in bootstrap commands rather than manual UI-only steps.

## Safe startup

This sequence preserves named volumes and `docker/minio/` data.

### Lakehouse infrastructure

```bash
docker compose \
  --env-file docker/.env \
  -f docker/docker-compose.yaml \
  --profile spark \
  up -d
```

Check status:

```bash
docker compose \
  --env-file docker/.env \
  -f docker/docker-compose.yaml \
  --profile spark \
  ps
```

The WSL Make target is:

```bash
make -C docker compose-up-all
```

### Build and initialize Airflow

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  build

docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  up airflow-init
```

### Start Airflow

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  up -d

docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  ps
```

The local UI defaults to `http://localhost:8080`. Unless overridden, bootstrap credentials are `airflow` / `airflow`.

After provisioning connections, unpause and trigger one DAG at a time. Start with the monthly ticker/company DAG because it is the smallest intended vertical slice.

## Ingestion smoke check

Run this only after resolving the Airflow/MinIO network and endpoint blockers and provisioning the four connections above.

Trigger the companies DAG from the UI or CLI:

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  exec airflow-apiserver \
  airflow dags trigger py-el-api-tickers
```

Verify these current-behavior signals:

1. Tasks finish in order: `start` -> `is_sec_api_available` -> `fetch_nasdaq_tickers_data` -> `finish`.
2. The task log reports `Stored <count> tickers` with a non-zero count.
3. MinIO contains `landing/tickers/nasdaq/<logical-date>.json`; this is the current key, not the target canonical `companies/run_id=...` key.
4. The object is valid JSON with the logged array length and a non-zero byte size.
5. Capture the Airflow DAG-run ID, logical date, object key, row count, byte size, and task-log link as run evidence.

This smoke check proves only the source-to-landing boundary. Do not infer bronze readiness from a successful object write because the current producer and bronze path contracts differ.

## Stop without deleting data

```bash
docker compose --env-file airflow/.env -f airflow/docker-compose.yaml down
docker compose --env-file docker/.env -f docker/docker-compose.yaml --profile spark down
```

Do not add `--volumes` unless metadata deletion is intentional.

## Full reset

> **Destructive:** These commands remove Airflow and Hive metadata volumes. MinIO objects require a separate explicit deletion because `docker/minio/` is a bind mount.

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  down --volumes --remove-orphans

docker compose \
  --env-file docker/.env \
  -f docker/docker-compose.yaml \
  --profile spark \
  down --volumes --remove-orphans
```

Back up important state before clearing `docker/minio/`. A reset of the metastore without a corresponding object-store strategy can leave orphaned or undiscoverable Iceberg state.

## Service endpoints

Defaults are controlled by `.env` files.

| Service | Default endpoint | Purpose |
|---|---|---|
| Airflow | `http://localhost:8080` | DAG administration |
| MinIO S3 API | `http://localhost:9000` | Object API |
| MinIO console | `http://localhost:9001` | Object browser |
| Hive Metastore | `thrift://localhost:9083` | Table metadata |
| Spark master | `spark://localhost:7077` | Job submission |
| Spark master UI | `http://localhost:8081` | Cluster status |
| Spark Thrift | `localhost:${SPARK_THRIFT_PORT_EXPOSE}` | JDBC/ODBC for dbt; `50234` in the checked-in example, `10000` as the Compose fallback |
| Flower, optional | `http://localhost:5555` | Celery monitoring |

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  --profile flower \
  up -d flower
```

## Historical OHLC import

The uploader expects `HISTORICAL_DATA_DIR` to contain subdirectories `1`, `2`, and `3`.

```bash
make -C scripts import-historical-data
```

or:

```bash
python3 scripts/batch/EL_his_eod_prices.py
```

It uploads each source file and writes `_manifest.json` with source and ingestion metadata.

## Spark applications

Target submission shape:

```bash
docker compose \
  --env-file docker/.env \
  -f docker/docker-compose.yaml \
  exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-apps/<application>.py
```

This command is not currently executable for the batch jobs: code mounts, image dependencies, and catalog names are not aligned. See the P0 platform boundary in [Implementation status](implementation-status.md) rather than treating submission failure as an application-data error.

## Great Expectations

Target commands:

```bash
python3 scripts/batch/data_validation/run_validate.py bronze
python3 scripts/batch/data_validation/run_validate.py silver
```

Generated validations and Data Docs live below `scripts/batch/gx/uncommitted/` and are ignored by Git.

Persisted bronze artifacts exist, but the validation entry point and silver suite construction require repair. See [Implementation status](implementation-status.md) and [Quality and observability](quality-and-observability.md).

## dbt

The project expects profile `dbt_stock` connecting to Spark Thrift.

```bash
cd dbt
dbt deps
dbt debug
dbt seed
dbt snapshot
dbt build
```

The dbt project is not currently runnable to a verified Iceberg gold layer. See [Implementation status](implementation-status.md); target grains, keys, and relationships are defined in [Gold data model](gold-data-model.md).

## Troubleshooting

### Container cannot resolve `minio` or `hive-metastore`

```bash
docker network inspect lakehouse-network
```

Confirm producer and dependency share the network.

### Compose warns about an unset port

Compare `docker/.env` with its example. `SPARK_THRIFT_UI_PORT_EXPOSE` is referenced by Compose but absent from the current example.

### Airflow DAG import fails

```bash
docker compose \
  --env-file airflow/.env \
  -f airflow/docker-compose.yaml \
  logs airflow-dag-processor
```

Confirm HTTP and Amazon provider packages are installed. They are imported by DAGs but not explicitly pinned in `airflow/requirements.txt`.

### Ingestion succeeds but bronze finds no input

Current DAG object keys do not match the canonical `run_id=...` layout expected by the bronze scanner. This requires a producer/consumer contract fix, not a service restart.

### dbt cannot find `dbt_stock`

Create `~/.dbt/profiles.yml` or pass `--profiles-dir` containing that profile. No reusable example is checked in yet.

## Security

- Never commit `.env`, Airflow connection exports, API keys, or MinIO credentials.
- Default Airflow credentials are local-development only.
- Compose uses an empty Fernet key and test JWT secret.
- MinIO uses local HTTP without TLS.
- Host ports lack production-grade access controls.
- Pin images instead of relying on `latest`.

## Related documents

- [Architecture](architecture.md)
- [Gold data model](gold-data-model.md)
- [Incremental loading](incremental-loading.md)
- [Implementation status](implementation-status.md)
- [Documentation index](README.md)
- [Project README](../README.md)
