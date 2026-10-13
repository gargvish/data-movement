# Phase 2 — Kafka CDC → Bronze Iceberg (via Spark Structured Streaming)

Goal: land every Debezium CDC event in an append-only Bronze Iceberg table — a durable, replayable history of every row change, physically stored as Parquet on object storage, discoverable through an Iceberg REST catalog.

By the end of Phase 2, any INSERT/UPDATE/DELETE in Postgres shows up as a new row in `bronze.vehicle_cdc`, `bronze.registration_cdc`, or `bronze.payment_cdc` within ~5 seconds, queryable from Spark SQL.

Depends on Phase 1 being up (`dm-postgres`, `dm-kafka`, `dm-connect` healthy, Debezium connector `RUNNING`).

---

## Architecture

```
Kafka  (db.public.<table> topics from Phase 1)
   |
   v
Spark Structured Streaming   (dm-spark)
   |  parse Debezium envelope, extract tenant_id, add kafka metadata
   v
Iceberg REST Catalog   (dm-iceberg-rest, JDBC backend)
   |  issues metadata.json pointer updates on each commit
   v
Postgres: iceberg_catalog DB   (reuses dm-postgres)
   |  one row per table: metadata_location pointer

Parquet data + metadata files live on:
RustFS (S3-compatible)   (dm-rustfs)
   s3://warehouse/bronze/<table>/data/*.parquet
   s3://warehouse/bronze/<table>/metadata/*.metadata.json
   s3a://warehouse/_checkpoints/<table>/    (Spark streaming state)
```

Services added:

| Container          | Image                             | Role                                        | Host port       |
|--------------------|-----------------------------------|---------------------------------------------|-----------------|
| `dm-rustfs`        | `rustfs/rustfs:latest`            | S3-compatible object storage                | 9000, 9001      |
| `dm-rustfs-init`   | `amazon/aws-cli:latest`           | One-shot bucket creator                     | —               |
| `dm-iceberg-rest`  | `tabulario/iceberg-rest:1.6.0`    | Iceberg REST catalog, JDBC backend          | 8181            |
| `dm-spark`         | `apache/spark:3.5.3`              | Local-mode Spark, long-running shell host   | —               |

---

## Files added in this phase

```
db/iceberg_catalog.sql                  # creates iceberg user + iceberg_catalog DB
spark/conf/spark-defaults.conf          # Iceberg catalog, S3FileIO, S3A, --packages
spark/sql/bronze_tables.sql             # DDL for the three Bronze tables
spark/jobs/kafka_to_bronze.py           # the streaming job
```

---

## Step 1 — Object storage (RustFS)

RustFS is a Rust rewrite of MinIO, S3-API-compatible, same ports (9000 API / 9001 console).

```bash
docker compose up -d rustfs rustfs-init
```

The `rustfs-init` container runs once with the AWS CLI, waits for RustFS, creates the `warehouse` bucket, exits. From the host:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 ls
# warehouse
```

Browser console: http://localhost:9001  (`rustfsadmin` / `rustfsadmin`).

**Why RustFS, not MinIO:** MinIO removed its Docker Hub images in late 2024 and the quay.io replacements require authentication. RustFS is a drop-in S3-compatible server, publicly available on Docker Hub, uses the same `MINIO_*`-style env vars (we use the new `RUSTFS_*` names), and works unchanged with the AWS CLI and Iceberg's `S3FileIO`.

**Why AWS CLI for bucket creation, not `mc`:** MinIO's `mc` client has the same auth-wall as the server. AWS CLI is public, is already the canonical S3 client, and works against any S3-compatible endpoint via `--endpoint-url`.

---

## Step 2 — Iceberg REST catalog

Create a dedicated Postgres database for the catalog's JDBC backend:

```bash
docker exec -i dm-postgres psql -U dmv -d postgres < db/iceberg_catalog.sql
```

Start the REST catalog:

```bash
docker compose up -d iceberg-rest
```

Verify:

```bash
curl -s http://localhost:8181/v1/config
# {"defaults":{},"overrides":{}}

curl -s http://localhost:8181/v1/namespaces
# {"namespaces":[],"next-page-token":null}

docker exec dm-postgres psql -U iceberg -d iceberg_catalog -c "\dt"
# iceberg_namespace_properties | iceberg_tables
```

**Catalog config (via env vars in compose):** the Iceberg REST catalog config convention is `CATALOG_<property>`, with `__` in the env name becoming `.` in the property. So `CATALOG_S3_PATH__STYLE__ACCESS=true` sets the property `s3.path-style-access=true`.

| Env var                           | Property                      | Value                                      |
|-----------------------------------|-------------------------------|--------------------------------------------|
| `CATALOG_WAREHOUSE`               | `warehouse`                   | `s3://warehouse/`                          |
| `CATALOG_IO__IMPL`                | `io-impl`                     | `org.apache.iceberg.aws.s3.S3FileIO`       |
| `CATALOG_S3_ENDPOINT`             | `s3.endpoint`                 | `http://rustfs:9000`                       |
| `CATALOG_S3_PATH__STYLE__ACCESS`  | `s3.path-style-access`        | `true`                                     |
| `CATALOG_CATALOG__IMPL`           | `catalog-impl`                | `org.apache.iceberg.jdbc.JdbcCatalog`      |
| `CATALOG_URI`                     | `uri` (JDBC URL)              | `jdbc:postgresql://postgres:5432/iceberg_catalog` |
| `CATALOG_JDBC_USER`               | `jdbc.user`                   | `iceberg`                                  |
| `CATALOG_JDBC_PASSWORD`           | `jdbc.password`               | `iceberg`                                  |

**Why JDBC backend, not SQLite:** the original architecture called for a JDBC backend. Reusing `dm-postgres` (with a separate database `iceberg_catalog`) keeps the container count down and makes catalog state as durable as the operational DB.

**Why `path-style-access=true`:** MinIO/RustFS don't support AWS virtual-host–style URLs (`<bucket>.<endpoint>/key`). Path style (`<endpoint>/<bucket>/key`) is the compatibility mode and is required when talking to a non-AWS S3-compatible server.

### What's actually in the catalog's Postgres tables

After a table is created, the row in `iceberg_tables` looks roughly like:

```
catalog_name      | rest_backend
table_namespace   | bronze
table_name        | vehicle_cdc
metadata_location | s3://warehouse/bronze/vehicle_cdc/metadata/00042-....metadata.json
```

That's the whole catalog state for one table — one row, one pointer. The actual schema, partition spec, snapshot history, manifest list, data files all live on object storage, chained from `metadata_location`. See the deeper explanation in the git history / conversation notes.

A commit is a single `UPDATE iceberg_tables SET metadata_location=<new> WHERE ... AND metadata_location=<old>`. Two writers racing → one succeeds, the other retries. That's where Iceberg's ACID guarantee comes from.

---

## Step 3 — Spark

Start the Spark container (apache/spark:3.5.3, kept alive with `tail -f /dev/null` so we can `docker exec` into it):

```bash
docker compose up -d spark
```

First `spark-sql` invocation downloads all `--packages` into the `spark-ivy` volume (~130 MB, slow once, cached after):

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e 'SHOW CATALOGS;'
# demo
# spark_catalog
```

Both catalogs listed. `demo` is our REST catalog; `spark_catalog` is Spark's default session catalog (unused by us — the Hive warnings it logs are harmless).

### What's in `spark/conf/spark-defaults.conf`

Three concerns:

**1. Packages pulled by Ivy on first run:**

| Package                                                          | Role                                              |
|------------------------------------------------------------------|---------------------------------------------------|
| `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1`        | Iceberg SQL extensions, catalog adapter           |
| `org.apache.iceberg:iceberg-aws-bundle:1.6.1`                    | AWS SDK v2 + `S3FileIO` (talks to RustFS)         |
| `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3`               | Kafka source for Structured Streaming             |
| `org.apache.hadoop:hadoop-aws:3.3.4`                             | S3A filesystem (used **only** by streaming checkpoints) |

Spark 3.5 / Scala 2.12 / Iceberg 1.6 / Hadoop 3.3.4 — these are all pinned to compatible versions and must not be changed independently.

**2. Iceberg catalog named `demo` wired to the REST service:**

```
spark.sql.extensions                           org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.demo                         org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.demo.type                    rest
spark.sql.catalog.demo.uri                     http://iceberg-rest:8181
spark.sql.catalog.demo.io-impl                 org.apache.iceberg.aws.s3.S3FileIO
spark.sql.catalog.demo.s3.endpoint             http://rustfs:9000
spark.sql.catalog.demo.s3.path-style-access    true
spark.sql.catalog.demo.s3.access-key-id        rustfsadmin
spark.sql.catalog.demo.s3.secret-access-key    rustfsadmin
spark.sql.catalog.demo.client.region           us-east-1
spark.sql.defaultCatalog                       demo
```

Same S3 config as the REST catalog has — because when Spark reads an Iceberg table, the Spark executor opens the Parquet files directly via `S3FileIO` (the REST catalog is only involved in resolving the current `metadata.json` pointer).

**3. Hadoop S3A config, used only by streaming checkpoints:**

```
spark.hadoop.fs.s3a.endpoint                   http://rustfs:9000
spark.hadoop.fs.s3a.access.key                 rustfsadmin
spark.hadoop.fs.s3a.secret.key                 rustfsadmin
spark.hadoop.fs.s3a.path.style.access          true
spark.hadoop.fs.s3a.connection.ssl.enabled     false
spark.hadoop.fs.s3a.aws.credentials.provider   org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider
```

Why two S3 config dialects? Iceberg uses its own `S3FileIO` (AWS SDK v2, Iceberg-native). Spark's Structured Streaming checkpoint mechanism uses the Hadoop FileSystem API, which only knows about `s3a://` paths and `fs.s3a.*` properties. The two code paths coexist cleanly and both talk to the same RustFS instance.

---

## Step 4 — Bronze table DDL

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/bronze_tables.sql
```

Each of `bronze.vehicle_cdc`, `bronze.registration_cdc`, `bronze.payment_cdc` gets the same schema:

```sql
op              STRING          -- c | u | d | r
before_json     STRING          -- raw JSON, nullable
after_json      STRING          -- raw JSON, nullable
tenant_id       INT
source_lsn      BIGINT          -- authoritative order within the stream
source_txid     BIGINT
source_ts_ms    BIGINT
source_snapshot STRING          -- "true"/"false"/"first"/"last"
ingest_ts       TIMESTAMP
kafka_topic     STRING
kafka_partition INT
kafka_offset    BIGINT
```

Partitioned by `tenant_id`.

**Why `before_json`/`after_json` as STRING, not STRUCT:** keeps the Bronze schema stable even if the source table schema evolves. Silver (next phase) will parse these into typed columns and handle version differences. Trading a bit of query ergonomics for schema-evolution flexibility.

**Why one Bronze table per source table** (not one unified `bronze.cdc_events` with a `source_table` column): simpler partitioning, each table's schema can diverge over time, each has its own compaction cadence, and queries don't need to filter by source.

Verify:

```bash
curl -s http://localhost:8181/v1/namespaces/bronze/tables
# {"identifiers":[{"namespace":["bronze"],"name":"payment_cdc"}, ...]}

AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 ls s3://warehouse/ --recursive
# bronze/payment_cdc/metadata/00000-....metadata.json
# bronze/registration_cdc/metadata/00000-....metadata.json
# bronze/vehicle_cdc/metadata/00000-....metadata.json
```

Three tiny metadata files, no data files yet (tables are empty).

---

## Step 5 — Spark Structured Streaming: Kafka → Bronze

Pre-create the Kafka topic for payments (Debezium only creates a topic the first time a row changes in that table; if the operational table has had no activity, the topic doesn't exist, and `readStream.subscribe` on a missing topic fails fast):

```bash
docker exec dm-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic db.public.payment --partitions 1 --replication-factor 1
```

Launch the streaming job detached:

```bash
docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/kafka_to_bronze.py \
   > /tmp/stream.log 2>&1"

# tail it
docker exec dm-spark tail -f /tmp/stream.log
```

What the job does, per topic:

1. `readStream.format("kafka")` subscribes to `db.public.<table>` from earliest offset.
2. Drops Kafka tombstone messages (`value IS NULL`).
3. Parses the Debezium envelope JSON with `get_json_object()` — no schema needed, so source schema evolution doesn't break the job.
4. Extracts `tenant_id` with `coalesce(after.tenant_id, before.tenant_id)` — `after` is null on deletes, `before` is null on inserts.
5. Keeps `before`/`after` as raw JSON strings.
6. Appends to `bronze.<table>_cdc` with `writeStream.format("iceberg").toTable(...)`.
7. 5-second micro-batch trigger. Each table has its own checkpoint at `s3a://warehouse/_checkpoints/bronze_<table>_cdc/`.

**Why one streaming query per table** (not a single query with `subscribePattern`): each table gets independent checkpointing. If one query falls behind or crashes, the others keep running. Operationally simpler to monitor and restart.

**Why `startingOffsets=earliest`:** backfills existing Kafka history on first run. On subsequent runs, the checkpoint takes over and this option is ignored.

### Verify

Query the Bronze tables while the stream runs:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  SELECT 'vehicle_cdc' AS t, tenant_id, op, count(*)
  FROM bronze.vehicle_cdc GROUP BY tenant_id, op
  UNION ALL
  SELECT 'registration_cdc', tenant_id, op, count(*)
  FROM bronze.registration_cdc GROUP BY tenant_id, op
  UNION ALL
  SELECT 'payment_cdc', tenant_id, op, count(*)
  FROM bronze.payment_cdc GROUP BY tenant_id, op
  ORDER BY 1, 2, 3;
"
```

New data files appear on each micro-batch commit:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 ls \
  s3://warehouse/bronze/registration_cdc/ --recursive
```

---

## Step 6 — Smoke test: end-to-end

With the streaming job running:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
INSERT INTO vehicle (tenant_id, vehicle_id, vin, make, model)
  VALUES (102, 10, '1HGBH41JXMN109186', 'Ford', 'F-150'),
         (103, 20, '2T1BU4EE4DC012345', 'Nissan', 'Altima');
INSERT INTO registration (tenant_id, registration_id, vehicle_id, status)
  VALUES (102, 2010, 10, 'ACTIVE'),
         (103, 3020, 20, 'ACTIVE');
INSERT INTO payment (tenant_id, payment_id, registration_id, amount)
  VALUES (102, 20100, 2010, 125.00),
         (103, 30200, 3020,  85.50);
UPDATE registration SET status='SUSPENDED', updated_at=now()
  WHERE tenant_id=102 AND registration_id=2010;
EOF
```

Within ~5–10 seconds the Bronze tables should show the new rows (ops `c` and `u` for the respective tenants). The data has traveled Postgres → Debezium → Kafka → Spark → Iceberg → RustFS.

---

## Gotchas hit during development

**1. MinIO moved behind auth.**
Both `docker.io/minio/minio` and `quay.io/minio/minio` now require authentication. Switched to `rustfs/rustfs` on Docker Hub, which is API-compatible. Bitnami's `minio` is also unavailable (Bitnami retired free Docker Hub images in August 2025).

**2. Single-file bind-mount broke after `Edit` rewrote the host file.**
Docker's bind mount of a single file tracks a specific inode. When Claude's `Edit` tool rewrites `spark-defaults.conf`, it writes to a new inode; the container still points at the old one, which `stat` reports as `Links: 0`. The symptom is `FileNotFoundException` from `spark-submit` even though the file is visible to `ls`. Fix: mount the whole `./spark/conf` directory instead of the single file. Directory mounts track by path, not inode.

**3. `db.public.payment` didn't exist as a Kafka topic.**
Debezium only creates a topic on first row-change. We never INSERTed a `payment` during Phase 1, so no topic existed, so `readStream.subscribe("db.public.payment")` failed with `UnknownTopicOrPartitionException`. Fix: pre-create the topic with `kafka-topics --create`. Could also use `subscribePattern` instead of `subscribe`, but that'd require a different stream topology.

**4. Checkpoint location can't use `S3FileIO`.**
Structured Streaming's checkpoint uses Hadoop FileSystem API, which only speaks `s3a://` (not Iceberg's `s3://` + `S3FileIO`). This is why we added `hadoop-aws:3.3.4` and the `spark.hadoop.fs.s3a.*` config. The two S3 code paths live side by side.

---

## Teardown

Stop and remove Phase 2 containers:

```bash
docker compose stop spark iceberg-rest rustfs rustfs-init
docker compose rm -f spark iceberg-rest rustfs rustfs-init
```

Nuke all data (object storage, ivy cache, catalog metadata rows in Postgres):

```bash
docker compose down -v   # wipes the whole stack including Phase 1
```

Partial reset — drop Iceberg tables and start fresh without losing Postgres operational data:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  DROP TABLE IF EXISTS bronze.vehicle_cdc PURGE;
  DROP TABLE IF EXISTS bronze.registration_cdc PURGE;
  DROP TABLE IF EXISTS bronze.payment_cdc PURGE;
  DROP NAMESPACE IF EXISTS bronze;
"
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 rm --recursive s3://warehouse/
```

---

## Recreating from a clean checkout (Phase 1 + Phase 2)

```bash
# Phase 1
docker compose up -d postgres
docker exec -i dm-postgres psql -U dmv -d dmv < db/schema.sql
docker compose up -d kafka connect
until curl -fsS http://localhost:8083/ >/dev/null; do sleep 2; done
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors --data @connect/dmv-postgres-connector.json

# Phase 2
docker compose up -d rustfs rustfs-init
docker exec -i dm-postgres psql -U dmv -d postgres < db/iceberg_catalog.sql
docker compose up -d iceberg-rest spark

# Pre-create any Kafka topic that will not get CDC traffic before the stream starts
docker exec dm-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --topic db.public.payment --partitions 1 --replication-factor 1 2>/dev/null || true

# Bronze DDL + streaming job
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/bronze_tables.sql
docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/kafka_to_bronze.py \
   > /tmp/stream.log 2>&1"
```

---

## What Phase 2 does not do

- No deduplication, upserts, or current-state view. Bronze is intentionally append-only — multiple rows per PK are expected (one per change). Silver (Phase 3) handles the `MERGE INTO` to maintain current state.
- No tenant routing — all tenants land in the same Bronze tables. Whale vs shared processing isolation comes later.
- Streaming job runs in Spark local mode, single JVM, inside one container. No HA, no cluster, no auto-restart. Fine for a laptop PoC.
- No schema evolution handling. If the Postgres schema changes, the raw JSON in `before`/`after` picks up the new fields automatically; Silver will need to adapt.
- No compaction. Iceberg's `rewriteDataFiles` and `expireSnapshots` would normally run on a cadence to keep file counts manageable.
