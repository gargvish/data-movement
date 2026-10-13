# Phase 4 — Silver → Gold (batch aggregations)

Goal: pre-compute query-ready tables for dashboards and ML features. Three Gold tables are rebuilt from scratch on each run, idempotently, via plain Spark SQL `INSERT OVERWRITE`.

By the end of Phase 4 you have a complete medallion stack — Postgres → Debezium → Kafka → Bronze → Silver → **Gold** — all running on your laptop.

Depends on Phase 3 (`silver.*` tables maintained by `bronze_to_silver.py`).

---

## Architecture

```
bronze.registration_cdc   silver.vehicle   silver.registration   silver.payment
         |                       |__________________|__________________|
         |                                        |
         |                                        v
         +---------------->  Spark SQL batch (silver_to_gold.py)
                                   INSERT OVERWRITE x 3
                                        |
                                        v
                            gold.daily_registrations
                            gold.tenant_revenue
                            gold.active_vehicles
```

No new containers in Phase 4. One Python script, three SQL queries, run on demand.

---

## Why batch, not streaming

Phases 2 and 3 are streaming because CDC → current-state is a continuous event problem — every row change needs to flow through within seconds. Gold aggregations are a different shape:

- Business cadence is naturally periodic (hourly, daily — rarely "immediately").
- A full scan over Silver is usually cheaper than an incremental maintenance plan, especially at PoC scale.
- Deterministic from the current Silver/Bronze state, so re-running is safe and the output is easy to reason about.
- Simpler mental model, simpler failure recovery (no checkpoints, no exactly-once protocol).

In production, this batch script would be driven by cron, Airflow, a Kubernetes CronJob, or dbt. For the PoC it's run manually.

---

## Files added in this phase

```
spark/sql/gold_tables.sql       # DDL for the three Gold tables
spark/jobs/silver_to_gold.py    # one-shot batch job, INSERT OVERWRITE x 3
```

---

## Step 1 — Gold DDL

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/gold_tables.sql
```

| Table | Grain | Partitioned by | Source |
|-------|-------|----------------|--------|
| `gold.daily_registrations` | `(tenant_id, event_date)` | `tenant_id` | **Bronze** `registration_cdc` |
| `gold.tenant_revenue`      | `(tenant_id)`             | — (tiny)    | **Silver** `payment` ⋈ `registration` |
| `gold.active_vehicles`     | `(tenant_id, vehicle_id)` | `tenant_id` | **Silver** `vehicle` ⋈ `registration` |

Verify:

```bash
curl -s http://localhost:8181/v1/namespaces/gold/tables
# active_vehicles | daily_registrations | tenant_revenue
```

---

## Step 2 — The batch job

Three queries, each a single `INSERT OVERWRITE`. Rerun semantics: each execution replaces the Gold table's contents entirely with the result of the current Silver/Bronze state. Partition-level overwrite is Iceberg's default for `INSERT OVERWRITE` on a partitioned table — but here the queries always produce the full result set across all partitions, so it's effectively a full rebuild.

### `gold.daily_registrations` — reads Bronze

```sql
SELECT
    event_date, tenant_id,
    sum(CASE WHEN op = 'c' THEN 1 ELSE 0 END)                                      AS new_registrations,
    sum(CASE WHEN op = 'u' AND after_status = 'SUSPENDED' THEN 1 ELSE 0 END)       AS suspensions,
    current_timestamp()                                                            AS computed_at
FROM (
    SELECT
        to_date(timestamp_millis(source_ts_ms)) AS event_date,
        tenant_id, op,
        get_json_object(after_json, '$.status') AS after_status
    FROM demo.bronze.registration_cdc
    WHERE op IN ('c', 'u')
)
GROUP BY event_date, tenant_id
```

**Why Bronze, not Silver:** Silver is current-state only — it can tell you a registration *is* suspended today, but not *when* it was created or suspended. For daily event counts you need history, which only Bronze has. The event timestamp comes from the Postgres commit time (`source.ts_ms`, Debezium metadata), converted to a date.

### `gold.tenant_revenue` — reads Silver

```sql
SELECT
    p.tenant_id,
    sum(p.amount)       AS total_revenue,
    count(*)            AS payment_count,
    current_timestamp() AS computed_at
FROM demo.silver.payment p
JOIN demo.silver.registration r
  ON p.tenant_id = r.tenant_id AND p.registration_id = r.registration_id
WHERE p.is_deleted = false
  AND r.is_deleted = false
  AND p.amount IS NOT NULL
GROUP BY p.tenant_id
```

**Join on the composite key:** every join inside Silver uses `(tenant_id, record_id)` — tenant_id is part of the primary key and ensures we don't accidentally join across tenants (critical for multi-tenant correctness).

**Why both `is_deleted` filters:** a payment linked to a soft-deleted registration should not count toward live revenue. Soft deletes aren't automatically pruned, so the Gold query must explicitly exclude them. This is the trade-off of soft delete: more rows in Silver, but every downstream query must remember to filter.

### `gold.active_vehicles` — reads Silver

```sql
SELECT
    v.tenant_id, v.vehicle_id, v.vin, v.make, v.model,
    r.registration_id, r.status AS registration_status,
    r.source_updated_at,
    current_timestamp() AS computed_at
FROM demo.silver.vehicle v
JOIN demo.silver.registration r
  ON v.tenant_id = r.tenant_id AND v.vehicle_id = r.vehicle_id
WHERE v.is_deleted = false
  AND r.is_deleted = false
  AND r.status = 'ACTIVE'
```

A dimension-style snapshot: one row per currently-active vehicle registration. Used by downstream BI to answer "what cars are registered right now?" and by ML pipelines as a feature dataset.

### Run

```bash
docker exec dm-spark /opt/spark/bin/spark-submit --master 'local[*]' \
  /opt/spark/jobs/silver_to_gold.py
```

Output:

```
rebuilding gold.daily_registrations
rebuilding gold.tenant_revenue
rebuilding gold.active_vehicles
done
```

---

## Step 3 — Query Gold

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  SELECT * FROM gold.daily_registrations ORDER BY event_date, tenant_id;
  SELECT * FROM gold.tenant_revenue ORDER BY tenant_id;
  SELECT * FROM gold.active_vehicles ORDER BY tenant_id, vehicle_id;
"
```

Making the data more interesting (fresh payment on a live registration):

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
INSERT INTO payment (tenant_id, payment_id, registration_id, amount)
  VALUES (103, 30300, 3020, 149.00);
EOF

# wait a few seconds for the streaming jobs to update Silver
sleep 10

# recompute Gold
docker exec dm-spark /opt/spark/bin/spark-submit --master 'local[*]' \
  /opt/spark/jobs/silver_to_gold.py

docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e \
  "SELECT * FROM gold.tenant_revenue;"
```

Each rerun fully rebuilds Gold from the current Silver state. No partial updates, no race conditions with the streaming jobs — Silver is a consistent snapshot at query time.

---

## Idempotency and consistency

Three guarantees worth being explicit about:

1. **Idempotent rerun.** Running the batch job twice in a row produces the same Gold contents, regardless of what streaming has done in between. `INSERT OVERWRITE` discards all existing rows.
2. **Isolation from streaming.** The batch reads Silver tables as of a single Iceberg snapshot. If the streaming job commits a new Silver snapshot mid-run, the batch sees a consistent earlier version. This is pure Iceberg table semantics — no coordination required.
3. **Transactional writes.** The `INSERT OVERWRITE` is one Iceberg commit per Gold table. Readers always see either the old Gold snapshot or the new one, never a partial state.

---

## No new gotchas

Phase 4 is deliberately boring — no new containers, no new config, no new framework surface area. The infrastructure built in phases 1–3 does the heavy lifting; Gold is just three SQL queries on top.

---

## Teardown

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
  DROP TABLE IF EXISTS gold.daily_registrations PURGE;
  DROP TABLE IF EXISTS gold.tenant_revenue      PURGE;
  DROP TABLE IF EXISTS gold.active_vehicles     PURGE;
  DROP NAMESPACE IF EXISTS gold;
"
```

(No checkpoint directories to clean — batch jobs don't have them.)

---

## Recreating from a clean checkout (Phase 1 + 2 + 3 + 4)

```bash
# Phases 1 + 2 + 3 — see those docs
# ... all services up, Debezium registered, Bronze + Silver streams running ...

# Phase 4
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/gold_tables.sql

docker exec dm-spark /opt/spark/bin/spark-submit --master 'local[*]' \
  /opt/spark/jobs/silver_to_gold.py
```

Running Gold on a schedule in production:

```bash
# Simplest option: cron on the host every 15 minutes
*/15 * * * * cd /path/to/data-movement && docker exec dm-spark \
  /opt/spark/bin/spark-submit --master local[*] /opt/spark/jobs/silver_to_gold.py \
  >> /var/log/dmv-gold.log 2>&1

# For dependencies / backfills / observability, use Airflow or a workflow engine.
```

---

## What Phase 4 does not do

- **No incremental maintenance.** Each rerun scans the full Silver/Bronze state. Fine at PoC scale (seconds), would need rewriting for billions of rows (window-based incremental, or Iceberg's `CHANGELOG` scans).
- **No scheduling.** The job runs when you invoke it. Production would add a scheduler.
- **No data quality tests.** Gold table expectations (row counts, non-null columns, referential integrity against Silver) should be checked by a separate quality layer — great-expectations, Soda, dbt tests.
- **No compaction / snapshot expiry on Gold.** `INSERT OVERWRITE` churns snapshots every run. Over time, `expireSnapshots` should prune them.
- **No concept of "late" data.** `daily_registrations` groups by `source.ts_ms` from Postgres, which is authoritative, so there's nothing late — but Bronze would still need to be fully re-read on every run to capture any out-of-order events.
