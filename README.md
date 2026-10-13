# data-movement

A laptop-scale proof-of-concept for a multi-tenant CDC medallion pipeline:
Postgres → Debezium → Kafka → Spark Structured Streaming → Iceberg (Bronze / Silver / Gold) on S3-compatible object storage.

Everything runs in Docker Compose. No cloud account, no managed services.

---

## Stack

| Component           | Image                            | Role                                             |
|---------------------|----------------------------------|--------------------------------------------------|
| `dm-postgres`       | `postgres:16.4`                  | Operational DB (`wal_level=logical`) + Iceberg JDBC catalog backend |
| `dm-kafka`          | `confluentinc/cp-kafka:7.7.1`    | Event transport (KRaft, single broker)           |
| `dm-connect`        | `debezium/connect:2.7.3.Final`   | Debezium Postgres connector                      |
| `dm-rustfs`         | `rustfs/rustfs:latest`           | S3-compatible object storage (MinIO alternative) |
| `dm-iceberg-rest`   | `tabulario/iceberg-rest:1.6.0`   | Iceberg REST catalog                             |
| `dm-spark`          | `apache/spark:3.5.3`             | Spark Structured Streaming + batch jobs          |

---

## Repo layout

```
docker-compose.yml                     # all services + volumes
connect/dmv-postgres-connector.json    # Debezium connector config
db/
  schema.sql                           # source tables, debezium role, publication
  iceberg_catalog.sql                  # iceberg user + catalog DB
  debezium_signal.sql                  # signaling table for incremental snapshots
  migrations/                          # Postgres schema migrations
spark/
  conf/spark-defaults.conf             # Iceberg + S3A config
  sql/
    bronze_tables.sql                  # append-only CDC history
    silver_tables.sql                  # current state per PK
    gold_tables.sql                    # aggregates
    migrations/                        # Iceberg schema migrations
  jobs/
    kafka_to_bronze.py                 # Phase 2 streaming
    bronze_to_silver.py                # Phase 3 streaming (MERGE INTO)
    silver_to_gold.py                  # Phase 4 batch (INSERT OVERWRITE)
docs/                                  # phase + demo walkthroughs
```

---

## Documentation

Each phase and demo has its own step-by-step walkthrough with architecture, config rationale, and gotchas.

| Doc | What it covers |
|---|---|
| [`docs/phase-1.md`](docs/phase-1.md) | Postgres → Debezium → Kafka |
| [`docs/phase-2.md`](docs/phase-2.md) | Kafka → Bronze Iceberg (Spark Structured Streaming) |
| [`docs/phase-3.md`](docs/phase-3.md) | Bronze → Silver via `MERGE INTO` (current state, idempotent) |
| [`docs/phase-4.md`](docs/phase-4.md) | Silver → Gold batch aggregations |
| [`docs/demo-3-tenant-router-design.md`](docs/demo-3-tenant-router-design.md) | Whale vs shared tenant isolation (design only, not implemented) |
| [`docs/demo-4-schema-evolution.md`](docs/demo-4-schema-evolution.md) | New column flow through the stack + Debezium incremental snapshot backfill |
| [`docs/demo-5-tenant-isolation-and-deletion.md`](docs/demo-5-tenant-isolation-and-deletion.md) | Partition pruning + physical deletion (expire snapshots, orphan files) |
| [`docs/security-design.md`](docs/security-design.md) | Security design (not implemented) |

Start with `phase-1.md` and go in order — each phase builds on the previous.

---

## Quickstart

Bring up everything:

```bash
docker compose up -d
```

Apply the Postgres schema and Iceberg catalog DB:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv < db/schema.sql
docker exec -i dm-postgres psql -U dmv -d dmv < db/iceberg_catalog.sql
```

Register the Debezium connector (wait for Connect to be healthy first):

```bash
until curl -fsS http://localhost:8083/ >/dev/null; do sleep 2; done
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors \
  --data @connect/dmv-postgres-connector.json
```

Create the Iceberg tables:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -f /opt/spark/sql/bronze_tables.sql
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -f /opt/spark/sql/silver_tables.sql
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -f /opt/spark/sql/gold_tables.sql
```

Run the streaming jobs (each in its own terminal, or background):

```bash
docker exec dm-spark /opt/spark/bin/spark-submit /opt/spark/jobs/kafka_to_bronze.py
docker exec dm-spark /opt/spark/bin/spark-submit /opt/spark/jobs/bronze_to_silver.py
```

Run the Gold batch on demand:

```bash
docker exec dm-spark /opt/spark/bin/spark-submit /opt/spark/jobs/silver_to_gold.py
```

See the per-phase docs for verification queries, teardown, and the full explanation of why each config value is what it is.

---

## Host ports

| Service              | Port         |
|----------------------|--------------|
| Postgres             | 5432         |
| Kafka (host client)  | 9092         |
| Kafka Connect REST   | 8083         |
| RustFS S3 API        | 9000         |
| RustFS console       | 9001         |
| Iceberg REST catalog | 8181         |

---

## Teardown

```bash
docker compose down       # keep volumes
docker compose down -v    # nuke Postgres, Kafka, RustFS data
```
