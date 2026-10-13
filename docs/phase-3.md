# Phase 3 — Bronze → Silver (current state via MERGE INTO)

Goal: maintain a **current-state** table per source — one row per `(tenant_id, record_id)` — by streaming from Bronze and running `MERGE INTO` on each micro-batch.

By the end of Phase 3, any INSERT/UPDATE/DELETE in Postgres converges Silver to the final state within ~5–10 seconds, Silver is replay-safe (reprocessing the same Bronze rows is a no-op), and deletes are soft (`is_deleted=true`) rather than hard.

Depends on Phase 2 being up and the Kafka→Bronze streaming job (`kafka_to_bronze.py`) running.

---

## Architecture

```
bronze.<table>_cdc   (append-only CDC events, from Phase 2)
   |
   | Iceberg streaming source (reads newly-committed Bronze snapshots)
   v
Spark Structured Streaming
   |  foreachBatch:
   |    1. dedupe within batch by row_number() OVER (PARTITION BY pk ORDER BY source_lsn DESC)
   |    2. MERGE INTO silver.<table> ...
   |         WHEN MATCHED AND s.source_lsn > t.source_version THEN UPDATE *
   |         WHEN NOT MATCHED THEN INSERT *
   v
silver.<table>   (one row per primary key, idempotent)
```

No new containers in Phase 3 — the Bronze→Silver streaming job runs inside the same `dm-spark` container alongside the Phase 2 Kafka→Bronze job.

---

## Files added in this phase

```
spark/sql/silver_tables.sql           # DDL for silver.vehicle, silver.registration, silver.payment
spark/jobs/bronze_to_silver.py        # streaming job, one query per table
connect/dmv-postgres-connector.json   # +decimal.handling.mode=string
```

---

## Step 1 — Silver DDL

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/silver_tables.sql
```

Each table has typed columns matching the source plus three metadata columns:

| Column            | Meaning                                                   |
|-------------------|-----------------------------------------------------------|
| `source_version`  | Postgres LSN from the originating CDC event (BIGINT)      |
| `is_deleted`      | `true` after a soft-delete; otherwise `false`             |
| `silver_updated_at` | When the current row was last merged into Silver         |

Partitioned by `tenant_id` on all three tables.

**Why one Silver table per source** (not a unified wide table): schemas evolve independently, downstream consumers care about different tables at different times, partitioning stays simple.

**Why soft delete** (not `WHEN MATCHED THEN DELETE`):
- Keeps the row visible if the consumer wants to see it ("the registration still exists but is deleted").
- Simpler MERGE — one `WHEN MATCHED` branch instead of split by `op`.
- Reversible — a subsequent re-INSERT just flips `is_deleted` back to `false`.
- Downstream Gold queries add `WHERE NOT is_deleted` as a trivial filter.

Verify:

```bash
curl -s http://localhost:8181/v1/namespaces/silver/tables
# payment | registration | vehicle
```

---

## Step 2 — Bronze → Silver streaming job

```bash
docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/bronze_to_silver.py \
   > /tmp/silver.log 2>&1"

docker exec dm-spark tail -f /tmp/silver.log
```

### Shape of the job

- Three independent streaming queries, one per table.
- Each reads its source as an Iceberg streaming source:
  ```python
  spark.readStream.format("iceberg").load("demo.bronze.vehicle_cdc")
  ```
  On first run reads from the earliest Bronze snapshot (full backfill). On subsequent runs, the checkpoint takes over.
- `foreachBatch` is used because Spark's native streaming sinks don't support `MERGE INTO`. Each micro-batch becomes a short-lived temp view, and the MERGE runs as a one-shot SQL query against it.
- Checkpoint per query: `s3a://warehouse/_checkpoints/silver_<table>/`.

### The MERGE pattern (one example, `silver.vehicle`)

```sql
MERGE INTO demo.silver.vehicle t
USING (
  SELECT tenant_id, vehicle_id, vin, make, model,
         source_version, is_deleted, silver_updated_at
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT) AS vehicle_id,
      get_json_object(coalesce(after_json, before_json), '$.vin')   AS vin,
      get_json_object(coalesce(after_json, before_json), '$.make')  AS make,
      get_json_object(coalesce(after_json, before_json), '$.model') AS model,
      source_lsn            AS source_version,
      (op = 'd')            AS is_deleted,
      current_timestamp()   AS silver_updated_at,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT)
        ORDER BY source_lsn DESC
      ) AS rn
    FROM {view}
  ) WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.vehicle_id = s.vehicle_id
WHEN MATCHED AND s.source_version > t.source_version THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

Three load-bearing pieces:

1. **`coalesce(after_json, before_json)`** — on deletes, `after` is null. The `before` image carries the pre-delete column values, so we can still resolve the PK and keep the row populated.
2. **`row_number() OVER (...) WHERE rn = 1`** — collapses multiple events for the same key within one micro-batch to the most recent one. Without this, a single batch containing `c` + `u` + `u` for the same key would raise `MULTIPLE_MATCHED_ROWS_FOR_MERGE` from Iceberg.
3. **`AND s.source_version > t.source_version`** — only applies the update if the incoming LSN is strictly newer than what's already in Silver. This makes the whole pipeline **idempotent**: replaying the same Bronze rows is a no-op, out-of-order delivery is handled correctly, and a re-run after a crash does not corrupt state.

### Deletes

On `op='d'`, the SELECT produces a row with:
- Column values extracted from `before_json` (so Silver keeps a readable final row, not NULLs)
- `is_deleted = true`
- `source_version` bumped to the delete event's LSN

The MERGE path is unchanged — same `WHEN MATCHED ... UPDATE SET *`. A re-INSERT of the same PK later just updates the row with `is_deleted = false`.

### LSN as the authoritative version

Postgres LSN (log sequence number) is strictly monotonic within a cluster and reflects commit order. Debezium exposes it as `source.lsn`. We use it as the single ordering/versioning key throughout:

- Dedup within batch: `ORDER BY source_lsn DESC`
- MERGE guard: `s.source_version > t.source_version`
- Silver `source_version` column: the last LSN that modified this row

Using `ingest_ts` or `source_ts_ms` for ordering would be wrong — clock skew, batch reordering, and intra-second commits all break ties incorrectly. LSN is the only field that is guaranteed to be strictly monotonic per source commit.

---

## Step 3 — Smoke test

With both streaming jobs running:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
-- Three rapid changes to one key: should converge to the final values
INSERT INTO vehicle (tenant_id, vehicle_id, vin, make, model)
  VALUES (102, 11, '1FTFW1EV0DKF12345', 'Chevrolet', 'Silverado');
UPDATE vehicle SET model='Silverado 1500' WHERE tenant_id=102 AND vehicle_id=11;
UPDATE vehicle SET make='Chevy' WHERE tenant_id=102 AND vehicle_id=11;
EOF
```

After ~5–10 seconds:

```bash
# Bronze: three append-only history rows with ascending LSNs
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  SELECT op, source_lsn, after_json
  FROM bronze.vehicle_cdc
  WHERE tenant_id=102 AND get_json_object(after_json, '\$.vehicle_id')='11'
  ORDER BY source_lsn;
"
# c | 31263336 | {"make":"Chevrolet", "model":"Silverado",      ...}
# u | 31264152 | {"make":"Chevrolet", "model":"Silverado 1500", ...}
# u | 31264392 | {"make":"Chevy",     "model":"Silverado 1500", ...}

# Silver: single row with final values, source_version = max LSN
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  SELECT tenant_id, vehicle_id, make, model, source_version
  FROM silver.vehicle WHERE tenant_id=102 AND vehicle_id=11;
"
# 102 | 11 | Chevy | Silverado 1500 | 31264392
```

Delete semantics:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
DELETE FROM vehicle WHERE tenant_id=102 AND vehicle_id=11;
EOF

docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  SELECT tenant_id, vehicle_id, make, model, is_deleted, source_version
  FROM silver.vehicle WHERE tenant_id=102 AND vehicle_id=11;
"
# 102 | 11 | Chevy | Silverado 1500 | true | <higher LSN>
```

Row still visible, values preserved from before the delete, `is_deleted=true`, `source_version` bumped.

---

## Gotchas hit during development

**1. `foreachBatch` runs in a cloned SparkSession.**
Views registered via `batch_df.createOrReplaceTempView(name)` are scoped to `batch_df.sparkSession`, not the module-level `spark` singleton. Calling `spark.sql(...)` from inside `foreachBatch` raises `TABLE_OR_VIEW_NOT_FOUND`. Fix: use `batch_df.sparkSession.sql(...)` for both the view and the SQL.

**2. Debezium encodes DECIMAL as base64 by default.**
With `decimal.handling.mode=precise` (the default), `payment.amount` arrives in Kafka as `"amount":"IWY="` — base64 of the two's-complement big-endian bytes, scaled by the column scale. Unreadable in Bronze and NULL after `CAST(... AS DECIMAL)` in Silver. Fix: `"decimal.handling.mode": "string"` in the connector config. New events arrive as `"amount":"299.99"`. Historical events in Bronze keep the base64 encoding — in production you'd re-snapshot or run a one-off backfill.

**3. Multiple rows for the same key within one MERGE batch raises `MULTIPLE_MATCHED_ROWS_FOR_MERGE`.**
A single micro-batch can carry `c` + `u` + `u` for the same key. The `row_number() OVER (PARTITION BY pk ORDER BY source_lsn DESC) WHERE rn = 1` dedup is mandatory, not optional — the Iceberg MERGE spec rejects a source with more than one row per matched target row.

---

## Teardown

Stop just the Silver job:

```bash
docker exec dm-spark bash -c "pkill -f bronze_to_silver"
```

Drop Silver tables but leave Bronze:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  DROP TABLE IF EXISTS silver.vehicle PURGE;
  DROP TABLE IF EXISTS silver.registration PURGE;
  DROP TABLE IF EXISTS silver.payment PURGE;
  DROP NAMESPACE IF EXISTS silver;
"
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 rm --recursive s3://warehouse/_checkpoints/silver_vehicle
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 rm --recursive s3://warehouse/_checkpoints/silver_registration
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 rm --recursive s3://warehouse/_checkpoints/silver_payment
```

---

## Recreating from a clean checkout (Phase 1 + 2 + 3)

```bash
# Phases 1 + 2 (see those docs)
# ... postgres, kafka, connect, rustfs, iceberg-rest, spark up ...
# ... Debezium connector registered, Bronze tables created,
#     kafka_to_bronze.py streaming ...

# Phase 3
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/silver_tables.sql

docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/bronze_to_silver.py \
   > /tmp/silver.log 2>&1"
```

---

## What Phase 3 does not do

- No Gold aggregations. Silver is current-state only; `daily_registrations`, `registration_revenue`, `active_vehicles` are Phase 4.
- No automatic schema evolution. If the source table adds a column, Silver DDL and the MERGE SQL both need manual updates. Bronze absorbs it transparently (JSON strings).
- No handling for the base64 historical payments. They stay NULL in Silver. A production backfill would re-snapshot the connector or back-populate via a one-off script.
- No compaction or snapshot expiry on Silver. Iceberg's `rewriteDataFiles` + `expireSnapshots` should run on a cadence.
- The Silver `source_updated_at` for `registration` is parsed via `timestamp_micros()`. Debezium's microseconds format assumes `time.precision.mode=adaptive_time_microseconds` (the default for `pgoutput`). If the source column type or Debezium config changes, the parse breaks.
