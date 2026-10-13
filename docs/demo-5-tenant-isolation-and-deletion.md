# Demo 5 — Tenant isolation + physical deletion

Goal: demonstrate the gap between "SQL `DELETE` succeeded" and "the bytes are actually gone from object storage." This is a compliance-relevant distinction (GDPR right-to-be-forgotten, HIPAA, etc.) and a common source of audit failure.

Three things get exercised:
1. **Logical tenant isolation** via partition pruning — a query with `WHERE tenant_id = X` reads only one partition's files.
2. **DELETE is a metadata operation** — rows disappear from queries but Parquet files on object storage are untouched. Time-travel still exposes the "deleted" rows.
3. **Physical deletion requires an explicit pipeline** — `expire_snapshots` → optionally `remove_orphan_files` → optionally auto-prune metadata.

All three Silver tables (and Bronze and Gold for consistency) now have an auto-prune property enabled so metadata.json garbage doesn't accumulate forever.

---

## Files added or changed

```
spark/sql/bronze_tables.sql         # +TBLPROPERTIES for metadata hygiene
spark/sql/silver_tables.sql         # +TBLPROPERTIES for metadata hygiene
spark/sql/gold_tables.sql           # +TBLPROPERTIES for metadata hygiene
spark/sql/migrations/002_metadata_hygiene.sql   # ALTER for existing live tables
```

---

## Step 1 — Logical tenant isolation (partition pruning)

Silver tables are partitioned by `tenant_id`. Each tenant lives in its own directory on object storage:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 ls --recursive s3://warehouse/silver/vehicle/data/
# silver/vehicle/data/tenant_id=101/<file>.parquet
# silver/vehicle/data/tenant_id=102/<file>.parquet
# silver/vehicle/data/tenant_id=103/<file>.parquet
```

A tenant-scoped query gets physical pruning:

```sql
EXPLAIN FORMATTED SELECT * FROM silver.vehicle WHERE tenant_id = 102;
```

```
BatchScan demo.silver.vehicle
  filters=tenant_id = 102
```

Iceberg skips reads for `tenant_id = 101` and `tenant_id = 103`. 3× less I/O at our scale, linear with tenant count.

**What this is NOT:** security isolation. A query without the filter reads all tenants. Multi-tenant security must come from a layer above (per-tenant Spark catalog views, row-level security in the catalog, or a query gateway) — partition pruning is a performance feature, not an authorization boundary.

---

## Step 2 — Snapshot pre-delete state

For a before/after comparison we need the baseline:

```bash
AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin \
  aws --endpoint-url http://localhost:9000 s3 ls --recursive s3://warehouse/silver/vehicle/ \
  > /tmp/pre_delete.txt
awk '{sum+=$3; n++} END {printf "files=%d  bytes=%d\n", n, sum}' /tmp/pre_delete.txt
# files=55  bytes=289935
```

Observation: 55 files for 6 rows of data. The accumulation is from streaming churn — every CoW MERGE/UPDATE from `bronze_to_silver.py` writes a new Parquet (and new metadata.json). This is normal; we clean it up in step 4.

Breakdown:
- 20 data (Parquet) files total; only 3 are "live" (one per tenant, per the current snapshot's `.files` metadata).
- 10 metadata.json files (one per snapshot `00000` → `00009`).
- ~25 manifest + manifest-list Avro files.

---

## Step 3 — DELETE a tenant: SQL succeeds, bytes don't move

```sql
DELETE FROM silver.vehicle WHERE tenant_id = 101;
```

Query the current state — tenant 101 is gone:

```sql
SELECT tenant_id, count(*) FROM silver.vehicle GROUP BY tenant_id;
-- 102 | 2
-- 103 | 2
```

Current snapshot's `.files` metadata — no tenant 101 files:

```sql
SELECT partition.tenant_id, count(*)
FROM demo.silver.vehicle.files
GROUP BY partition.tenant_id;
-- 102 | 1
-- 103 | 1
```

A new snapshot was committed, with `operation = 'delete'`:

```sql
SELECT snapshot_id, operation, summary['total-records']
FROM demo.silver.vehicle.snapshots ORDER BY committed_at;
-- ... many older snapshots ...
-- 3759241295803751361 | delete | 4
```

### But the physical files are unchanged

```bash
aws --endpoint-url http://localhost:9000 s3 ls --recursive s3://warehouse/silver/vehicle/ \
  | awk '{sum+=$3; n++} END {printf "files=%d  bytes=%d\n", n, sum}'
# files=58  bytes=316561    ← INCREASED by 3 files (delete metadata), bytes went UP
```

### Time travel exposes the "deleted" rows

```sql
-- pick any snapshot_id from before the delete
SELECT tenant_id, count(*)
FROM silver.vehicle VERSION AS OF 6383743915889850431
GROUP BY tenant_id;
-- 101 | 2    ← still there
-- 102 | 2
-- 103 | 2
```

### And the Parquet files for tenant 101 are physically present

```bash
aws --endpoint-url http://localhost:9000 s3 ls --recursive \
  s3://warehouse/silver/vehicle/data/tenant_id=101/
# 5 Parquet files, ~13 KB total
```

**The compliance smoking gun.** SQL says tenant 101 is gone; `S3 ls` says the bytes are right there; time-travel can read them back any time.

### Why CoW on a partition-aligned DELETE adds metadata but no new data files

For `DELETE WHERE tenant_id = 101`:
- Iceberg scans the manifests for files whose partition tuple matches `tenant_id = 101`.
- Since we're deleting the *entire* partition's rows, no new Parquet files need to be written — the new snapshot just omits those files from its manifest list.
- The commit writes a new metadata.json, a new manifest list, and a new manifest (recording the "removed files" delta). Three new metadata files; zero new data files.
- Old data files for `tenant_id = 101` remain on disk because older snapshots still reference them via their manifest lists.

For a scattered DELETE (e.g. `WHERE vehicle_id % 2 = 0`), CoW rewrites every data file touched; see `docs/demo-4...` notes on CoW vs MoR.

---

## Step 4 — `expire_snapshots`: drop pointer history, free data bytes

```sql
CALL demo.system.expire_snapshots(
  table => 'silver.vehicle',
  older_than => TIMESTAMP '2099-01-01 00:00:00',
  retain_last => 1
);
```

Two things happen in modern Iceberg Spark (1.6+):

1. The `.snapshots` metadata table collapses to just the most recent snapshot. Older snapshot IDs can no longer be referenced:
   ```
   SELECT * FROM silver.vehicle VERSION AS OF 6383743915889850431;
   -- IllegalArgumentException: Cannot find snapshot with ID 6383743915889850431
   ```
2. By default (`clean_expired_files = true`), the procedure **also physically deletes** data files that were only referenced by the expired snapshots. In our case, all 5 of tenant 101's Parquet files, plus the 12 orphaned files from streaming churn, were deleted:
   ```
   files=58 → 16
   bytes=316561 → 111889
   ```

**Important:** `expire_snapshots` cleans **data files** of expired snapshots. It does NOT clean metadata.json files (those live in the table pointer history, separate concern — see step 5).

### The expire_snapshots knobs you actually tune

| Argument | Default | What it controls |
|---|---|---|
| `older_than` | 5 days ago | Only snapshots committed before this are candidates for expiration |
| `retain_last` | 1 | Keep at least N most-recent snapshots regardless of age |
| `clean_expired_files` | true | Physically delete data files only referenced by expired snapshots |

Common production pattern: schedule `expire_snapshots(table => 'silver.vehicle', older_than => now() - interval '7 days', retain_last => 10)` daily via cron/Airflow.

---

## Step 5 — `remove_orphan_files` safety floor + metadata.json residue

```sql
CALL demo.system.remove_orphan_files(
  table => 'silver.vehicle',
  older_than => TIMESTAMP '2099-01-01 00:00:00'
);
-- Error: Cannot remove orphan files with an interval less than 24 hours.
```

The SQL procedure form **enforces a 24-hour safety minimum**. The reason given in the error:

> Executing this procedure with a short interval may corrupt the table if other operations are happening at the same time.

A file that looks orphaned to the procedure could actually be a Parquet currently being written by a concurrent commit — a timing window exists between "data file written" and "metadata.json committed." The 24h floor is the default safety window.

Two ways around this when you need to be more aggressive:

**(a) Spark Actions API in Python** — overrides the floor explicitly:
```python
spark._jvm.org.apache.iceberg.spark.actions.SparkActions \
  .get(spark._jsparkSession).deleteOrphanFiles(table) \
  .olderThan(0) \
  .execute()
```
Use only when you're certain no concurrent writes are happening (e.g., maintenance window with streaming paused).

**(b) Wait 24 hours** between creating orphans and cleaning them. For scheduled cleanup pipelines this is often fine — just run the job daily with the default floor.

### The metadata.json residue problem

After `expire_snapshots`, we still have:
```
silver/vehicle/metadata/00000-....metadata.json
silver/vehicle/metadata/00001-....metadata.json
...
silver/vehicle/metadata/00010-....metadata.json
silver/vehicle/metadata/00011-....metadata.json   ← current pointer
```

10 historical metadata.json files, each ~2–13 KB. Nothing in `expire_snapshots` cleans these — they're table-pointer history, not snapshot data.

### Fix: enable metadata auto-pruning on every table

```sql
ALTER TABLE silver.vehicle SET TBLPROPERTIES (
  'write.metadata.delete-after-commit.enabled' = 'true',
  'write.metadata.previous-versions-max'       = '3'
);
```

Now every commit deletes metadata.json files older than the 3 most recent. Clean, automatic, no cron.

This is set in the DDL for all 9 tables (Bronze, Silver, Gold) in the repo. Migration `spark/sql/migrations/002_metadata_hygiene.sql` applies it to already-created tables.

---

## The full compliance picture

| Step | Logical state | Physical state | Compliant? |
|---|---|---|---|
| Pre-delete | 6 rows across 3 tenants | 20 Parquet files, 10 metadata.jsons | n/a |
| `DELETE WHERE tenant_id = 101` | 4 rows, tenant 101 absent | **Unchanged**; time-travel still returns 101 | **No** — bytes present |
| `expire_snapshots retain_last=1` | Time travel to pre-delete fails | Data files for 101 physically gone from RustFS | **Yes for user data**; metadata.jsons still linger |
| With `write.metadata.*` TBLPROPERTIES | — | Next commit auto-prunes old metadata.jsons | **Fully clean** |

---

## Production cleanup pipeline

For every table, the full hygiene pipeline is:

```
Table property (set once):
  write.metadata.delete-after-commit.enabled = true
  write.metadata.previous-versions-max       = 3

Scheduled daily job:
  CALL demo.system.expire_snapshots(table => <t>,
                                    older_than => now() - interval '7 days',
                                    retain_last => 10);
  CALL demo.system.rewrite_data_files(table => <t>);                 -- compact small files
  CALL demo.system.remove_orphan_files(table => <t>,
                                       older_than => now() - interval '2 days');
```

- Metadata hygiene is automatic (property).
- Snapshot and data-file cleanup is scheduled (snapshots accumulate during normal writes; need periodic pruning).
- Orphan cleanup is defense-in-depth (catches partial-commit artifacts).

For right-to-be-forgotten specifically: the DELETE + expire_snapshots sequence is the minimum, with `older_than` set tight enough that the specific delete falls out of the retention window.

---

## Gotchas hit during the demo

**1. SQL DELETE increased file count on disk.**
Counterintuitive but correct — the delete added a new metadata.json + manifest list + manifest for the new snapshot. Zero data files written (whole partition dropped). Bytes went UP, not down.

**2. `remove_orphan_files` has a 24-hour minimum interval in the SQL form.**
Safety against races with concurrent writers. Override via the Spark Actions API in Python, or wait 24 hours. Default is correct for most scheduled pipelines.

**3. `expire_snapshots` doesn't clean metadata.json files.**
Only the data files. Metadata residue requires the `write.metadata.delete-after-commit.enabled` property (or manual cleanup). Easy to miss — the data files were the "big" items, but 10 KB × 100 tables × 10K snapshots is a non-trivial amount of garbage over time.

**4. Partition pruning is isolation, but not security.**
`WHERE tenant_id = X` reads one partition efficiently. A query without the filter reads all tenants. True multi-tenant security requires authorization at the catalog layer or a query gateway — the table itself is physically shared.

---

## Recreating from a clean checkout

```bash
# Fresh checkout — bronze/silver/gold DDLs include the TBLPROPERTIES, so
# newly-created tables get metadata hygiene automatically. No migration needed.

docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/bronze_tables.sql
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/silver_tables.sql
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/gold_tables.sql

# For an existing deployment that pre-dates the TBLPROPERTIES change:
docker exec dm-spark /opt/spark/bin/spark-sql --master 'local[*]' \
  -f /opt/spark/sql/migrations/002_metadata_hygiene.sql
```

Then follow the demo steps above with any tenant_id.

---

## What this demo does not do

- **Schedule the cleanup pipeline.** `expire_snapshots`, `rewrite_data_files`, and `remove_orphan_files` all need to run on a cadence. We did them manually. Production uses Airflow, Dagster, cron, or similar.
- **Prove physical deletion to an auditor.** For GDPR you typically need an audit log — who ran the DELETE, when, which snapshots were expired, which files were removed. Add logging in the maintenance pipeline.
- **Handle MoR delete files.** Our tables are CoW. For a MoR table, scattered DELETEs produce position-delete files that `rewrite_data_files` compacts away. Different cleanup profile.
- **Guarantee pipeline isolation of streaming job.** Our Phase 2/3 streams were paused for this demo. In production, a concurrent streaming MERGE could race with the DELETE — Iceberg's optimistic concurrency means one of them retries. No data loss, but worth being aware of.
- **Drop a whole tenant across all tables atomically.** Our DELETE was on one table. A real tenant deletion would hit vehicle + registration + payment (+ Gold tables), each a separate commit. If you need atomicity, wrap the three in a single transaction (which Iceberg supports via multi-table transactions in Spark 3.3+).
