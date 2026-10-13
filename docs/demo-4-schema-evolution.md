# Demo 4 — Schema evolution

Goal: show how a new column added to a Postgres source table flows through the medallion stack, what breaks, and the production-grade pattern for handling it.

Three things get exercised:
1. **Bronze absorbs new columns transparently** because we store `after_json` / `before_json` as raw STRING. This is why that design choice was made.
2. **Silver evolves explicitly** — new column requires an `ALTER TABLE` + an update to the MERGE SQL.
3. **Existing rows need a backfill** — the ALTER itself does not emit CDC events for pre-existing rows, so historical Silver rows stay NULL.

Three production patterns were explored for the backfill: natural CDC propagation (via a Postgres UPDATE), surgical column-fill MERGE from Bronze, and Debezium incremental snapshot. The last is the one wired up in the repo.

---

## Files added or changed

```
db/migrations/001_vehicle_emission_class.sql
db/debezium_signal.sql                            # signaling table + grants + publication
spark/sql/migrations/001_silver_vehicle_emission_class.sql
connect/dmv-postgres-connector.json               # +signal.data.collection, +signal.enabled.channels
spark/jobs/bronze_to_silver.py                    # snapshot bypass + COALESCE on all 3 MERGEs
```

---

## Step 1 — Add the column to Postgres (migration)

```bash
docker exec -i dm-postgres psql -U dmv -d dmv < db/migrations/001_vehicle_emission_class.sql
```

Two statements:

```sql
ALTER TABLE vehicle ADD COLUMN emission_class VARCHAR(20) DEFAULT 'UNKNOWN';
UPDATE vehicle SET emission_class = 'UNKNOWN' WHERE emission_class IS NULL;
```

**Why DEFAULT:** In Postgres 11+ `ADD COLUMN ... DEFAULT <constant>` is a metadata-only operation (uses `attmissingval`). No heap rewrite, no row-level WAL. New rows get the default on INSERT; pre-ALTER rows return the default on SELECT.

**Why the follow-up UPDATE:** The `attmissingval` mechanism is a **read-time** resolution. The pre-ALTER rows are still physically NULL in the heap — nothing modified them. CDC captures physical row changes, so without this UPDATE, Debezium emits zero events for existing rows, and Silver stays NULL. The UPDATE forces each row to be rewritten, producing one CDC event per row, which Silver absorbs via the normal stream.

**Alternative to the UPDATE:** incremental snapshot (step 4). The UPDATE is the "simple cheap" option; the snapshot is the "scales to huge tables and is less invasive to WAL volume" option.

### What Debezium does on the ALTER itself

Essentially nothing visible. The `pgoutput` plugin sees the relation message and updates Debezium's in-memory schema cache. **No events are emitted to Kafka for the ALTER.** CDC captures DML, not DDL. The schema change is only "visible" in Kafka as the appearance of new fields in subsequent `after_json` payloads.

For Postgres specifically, Debezium's `include.schema.changes` option exists but is limited — unlike the MySQL connector, Postgres does not get a clean per-DDL schema-change topic. In practice, schema evolution for CDC is a **human-coordinated change**, not an auto-detected one.

---

## Step 2 — Confirm Bronze picked it up (no code change needed)

Insert and update a vehicle with the new column:

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
INSERT INTO vehicle (tenant_id, vehicle_id, vin, make, model, emission_class)
  VALUES (103, 21, '5YJ3E1EA7JF000021', 'Tesla', 'Model 3', 'ZEV');
UPDATE vehicle SET emission_class = 'EURO6'
  WHERE tenant_id = 102 AND vehicle_id = 10;
EOF
```

Query Bronze — the new field is in `after_json`:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' -e "
SELECT op, after_json FROM bronze.vehicle_cdc
WHERE get_json_object(after_json, '\$.emission_class') IS NOT NULL
ORDER BY source_lsn;
"
# c  {"tenant_id":103, ..., "emission_class":"ZEV"}
# u  {"tenant_id":102, ..., "emission_class":"EURO6"}
```

**Zero code changes** to the Bronze pipeline. The `kafka_to_bronze.py` job stores the raw JSON string — Debezium puts a new key in, Spark passes it through, the Bronze Parquet file has a slightly longer string value for `after_json`. That's it.

This is the entire payoff of the "raw JSON for before/after" design choice: Bronze is schema-stable against source evolution.

---

## Step 3 — Evolve Silver explicitly

Silver has typed columns, so it requires an explicit DDL change:

```bash
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/migrations/001_silver_vehicle_emission_class.sql
```

Then update the MERGE SQL in `spark/jobs/bronze_to_silver.py` — `MERGE_VEHICLE` must extract and emit the new column (otherwise the MERGE source lacks it and Silver's column stays NULL forever).

Restart the Silver stream:

```bash
docker exec dm-spark bash -c "pkill -f bronze_to_silver"
docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/bronze_to_silver.py > /tmp/silver.log 2>&1"
```

### Nuance: the LSN guard blocks replay

Simply restarting the stream (even wiping the checkpoint and reprocessing from Bronze start) will **not** backfill `emission_class` on existing Silver rows. Why: the Silver MERGE guard is `s.source_version > t.source_version`. The historical Bronze events for a given key have an LSN equal to what's already in Silver's `source_version` — so the guard refuses to re-apply them. This is correct behavior for idempotency.

Options to actually fill existing Silver rows:
- Natural CDC propagation via a Postgres UPDATE (step 1's second statement does this for all pre-ALTER rows at once).
- Surgical column-fill MERGE from Bronze (hand-written SQL that reads latest-per-key from Bronze and populates only NULL cells in Silver).
- Debezium incremental snapshot (step 4).

---

## Step 4 — Wire up Debezium incremental snapshot (production pattern)

Why: schema evolution recurs. Instead of hand-writing a backfill MERGE every time, make backfills a one-line operation that uses the existing pipeline.

### 4a. Create the signaling table in Postgres

```bash
docker exec -i dm-postgres psql -U dmv -d dmv < db/debezium_signal.sql
```

```sql
CREATE TABLE public.debezium_signal (
    id   VARCHAR(42)   PRIMARY KEY,
    type VARCHAR(32)   NOT NULL,
    data VARCHAR(2048)
);
ALTER TABLE public.debezium_signal REPLICA IDENTITY FULL;
GRANT SELECT, INSERT, UPDATE, DELETE ON public.debezium_signal TO debezium;
ALTER PUBLICATION dbz_pub ADD TABLE public.debezium_signal;
```

The connector both reads from (polls for new signals) and writes to (chunk watermarks) this table. Both DML privileges and inclusion in the publication are required.

### 4b. Point the connector at the signaling table

Add to `connect/dmv-postgres-connector.json`:

```json
"signal.data.collection": "public.debezium_signal",
"signal.enabled.channels": "source",
"incremental.snapshot.chunk.size": "1024"
```

PUT the updated config to the connector REST endpoint. The connector restarts and resumes from the committed WAL position (no re-snapshot of all tables).

### 4c. Fix the Silver MERGE for incremental-snapshot events

Important discovery during this demo: **incremental snapshot events in the Debezium Postgres connector have `source.lsn = NULL`.** The LSN of the chunk is in `source.sequence[1]`, not surfaced as a top-level field.

Consequence: the Silver MERGE guard `s.source_version > t.source_version` compares `NULL > number`, which is UNKNOWN (false), so snapshot events can't update existing Silver rows via the UPDATE branch. (The INSERT branch works because it has no guard — this is why the initial snapshot works on an empty Silver.)

Fix in `spark/jobs/bronze_to_silver.py` — all three MERGEs now include:

```sql
WHEN MATCHED AND (
    (s.op = 'r' AND s.source_snapshot = 'incremental')     -- snapshot bypass
    OR s.source_version > t.source_version                 -- LSN guard
) THEN UPDATE SET
    ... business columns ...,
    source_version = COALESCE(s.source_version, t.source_version)
WHEN NOT MATCHED THEN INSERT (
    ..., source_version, ...
) VALUES (
    ..., COALESCE(s.source_version, 0), ...
)
```

Behavior matrix:

| Event type | Match? | Guard | source_version after |
|---|---|---|---|
| Initial snapshot, empty Silver | NOT MATCHED → INSERT | n/a | source.lsn (real number) |
| Initial snapshot, matched row | MATCHED | LSN guard | source.lsn (newer) |
| Streaming, matched row | MATCHED | LSN guard | source.lsn (newer) |
| **Incremental snapshot, matched row** | **MATCHED** | **bypass true** | **t.source_version (preserved)** |
| Incremental snapshot, no match | NOT MATCHED → INSERT | n/a | 0 (sentinel) |

Three properties this gives us: no regression of `source_version`, idempotency on streaming replay preserved, snapshot content wins on incremental refresh.

### 4d. Trigger a backfill

```bash
docker exec -i dm-postgres psql -U dmv -d dmv <<'EOF'
INSERT INTO public.debezium_signal (id, type, data) VALUES (
  'backfill-vehicle-$(date +%s)',
  'execute-snapshot',
  '{"data-collections":["public.vehicle"],"type":"incremental"}'
);
EOF
```

Observe:
- 6 (one per Postgres vehicle row) `op=r / source_snapshot='incremental'` events appear in Bronze within seconds.
- Silver `silver_updated_at` refreshes on all 6 vehicles (proof the UPDATE branch fired via the snapshot bypass).
- Silver `source_version` is unchanged (preserved by COALESCE).
- Business columns match current Postgres state.

### 4e. Scope the snapshot (optional)

To re-ingest only a specific slice (e.g., one tenant), use `additional-conditions`:

```sql
INSERT INTO public.debezium_signal (id, type, data) VALUES (
  'backfill-tenant-101',
  'execute-snapshot',
  '{"data-collections":["public.vehicle"],"type":"incremental","additional-conditions":[{"data-collection":"public.vehicle","filter":"tenant_id = 101"}]}'
);
```

Only matching rows get re-emitted. Keeps Bronze growth proportional to the slice, not the full table.

---

## Operational pattern for future schema changes

The repeatable recipe:

```
1. Open a PR that:
   - adds the Postgres migration (ALTER + any bulk UPDATE)
   - adds the Silver migration (ALTER TABLE silver.<table> ADD COLUMN ...)
   - updates the MERGE SQL in bronze_to_silver.py
   - adds a debezium_signal INSERT to the deploy script (optional)

2. CI applies in order:
   - Postgres migration
   - Silver migration
   - Spark job restart (picks up new MERGE SQL)
   - Debezium signal INSERT (if used instead of bulk UPDATE)

3. Monitor:
   - Silver row count unchanged
   - New column populated per Postgres truth
   - Streaming lag returns to baseline after snapshot completes
```

---

## Gotchas hit during the demo

**1. `source.lsn` is NULL for incremental snapshot events.**
Guard `s.source_version > t.source_version` becomes `NULL > N` → UNKNOWN → false → MERGE skipped. Fix is the snapshot bypass + COALESCE pattern above. The initial snapshot does NOT exhibit this, because it has a real transaction LSN and typically runs against empty Silver (hitting the INSERT branch, which has no guard).

**2. DEFAULT does not flow through CDC for existing rows.**
`ALTER TABLE ... ADD COLUMN ... DEFAULT 'x'` is metadata-only in Pg 11+ — existing rows return 'x' on SELECT via `attmissingval`, but they are physically NULL in the heap. No CDC events fire for them. Silver stays NULL until each row is next modified (or until a backfill is triggered).

**3. ALTER with volatile DEFAULT (e.g. `DEFAULT now()`) rewrites the whole table.**
This takes an ACCESS EXCLUSIVE lock and generates full-table WAL. Avoid on large production tables. Standard pattern: ADD COLUMN with no / constant default, then bulk UPDATE in controlled batches.

**4. Watermark pollution in the signaling table.**
Each snapshot chunk writes a low + high watermark row (`type='snapshot-window-open'` / `'snapshot-window-close'`). For a huge snapshot that's thousands of rows accumulated in `debezium_signal`. Periodically prune: `DELETE FROM debezium_signal WHERE type != 'execute-snapshot' AND <date filter>`.

---

## Recreating from a clean checkout (schema evolution demo)

```bash
# Baseline (phases 1 + 2 + 3 + 4) already up ...

# Postgres migration + Silver migration
docker exec -i dm-postgres psql -U dmv -d dmv < db/migrations/001_vehicle_emission_class.sql
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/migrations/001_silver_vehicle_emission_class.sql

# Silver stream restart (picks up updated MERGE SQL that extracts emission_class)
docker exec dm-spark bash -c "pkill -f bronze_to_silver"
docker exec -d dm-spark bash -c \
  "/opt/spark/bin/spark-submit --master 'local[*]' /opt/spark/jobs/bronze_to_silver.py > /tmp/silver.log 2>&1"

# One-time: install the Debezium signaling table + update connector
docker exec -i dm-postgres psql -U dmv -d dmv < db/debezium_signal.sql
curl -X PUT -H "Content-Type: application/json" \
  http://localhost:8083/connectors/dmv-postgres/config \
  --data "$(python3 -c 'import json; print(json.dumps(json.load(open("connect/dmv-postgres-connector.json"))["config"]))')"
```

Future schema changes: `001_...` → `002_...` → `003_...` as a numbered sequence, applied in order.
