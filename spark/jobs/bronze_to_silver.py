"""
Bronze -> Silver: current-state tables maintained via MERGE INTO.

One Structured Streaming query per table. Each micro-batch:
  1. Reads newly-appended rows from bronze.<table>_cdc.
  2. Within the batch, keeps the row with max source_lsn per primary key
     (NULLS LAST, so streaming events win over snapshot events on the same key).
  3. MERGE INTO silver.<table>:
       - MATCHED + streaming: LSN guard (s.source_version > t.source_version)
       - MATCHED + incremental snapshot: bypass guard; apply content, keep t.source_version
       - NOT MATCHED: INSERT, sentinel source_version=0 if LSN is null
Deletes are soft: is_deleted=true with bumped source_version.
"""
from pyspark.sql import SparkSession, DataFrame

CHECKPOINT_BASE = "s3a://warehouse/_checkpoints"


# Each MERGE below:
#   - reads from a temp view {view} that holds one micro-batch of bronze rows
#   - uses coalesce(after_json, before_json) to resolve the PK (after is null on delete)
#   - deduplicates with row_number() over source_lsn DESC NULLS LAST
#   - snapshot bypass handles incremental-snapshot events (source.lsn is null in Debezium
#     Postgres connector for incremental snapshots)

MERGE_VEHICLE = """
MERGE INTO demo.silver.vehicle t
USING (
  SELECT
    tenant_id, vehicle_id, vin, make, model, emission_class,
    source_version, is_deleted, silver_updated_at, op, source_snapshot
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT) AS vehicle_id,
      get_json_object(coalesce(after_json, before_json), '$.vin')            AS vin,
      get_json_object(coalesce(after_json, before_json), '$.make')           AS make,
      get_json_object(coalesce(after_json, before_json), '$.model')          AS model,
      get_json_object(coalesce(after_json, before_json), '$.emission_class') AS emission_class,
      source_lsn           AS source_version,
      (op = 'd')           AS is_deleted,
      current_timestamp()  AS silver_updated_at,
      op,
      source_snapshot,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT)
        ORDER BY source_lsn DESC NULLS LAST
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.vehicle_id = s.vehicle_id
WHEN MATCHED AND (
    (s.op = 'r' AND s.source_snapshot = 'incremental')
    OR s.source_version > t.source_version
) THEN UPDATE SET
    vin               = s.vin,
    make              = s.make,
    model             = s.model,
    emission_class    = s.emission_class,
    source_version    = COALESCE(s.source_version, t.source_version),
    is_deleted        = s.is_deleted,
    silver_updated_at = s.silver_updated_at
WHEN NOT MATCHED THEN INSERT (
    tenant_id, vehicle_id, vin, make, model, emission_class,
    source_version, is_deleted, silver_updated_at
) VALUES (
    s.tenant_id, s.vehicle_id, s.vin, s.make, s.model, s.emission_class,
    COALESCE(s.source_version, 0), s.is_deleted, s.silver_updated_at
)
"""

MERGE_REGISTRATION = """
MERGE INTO demo.silver.registration t
USING (
  SELECT
    tenant_id, registration_id, vehicle_id, status, source_updated_at,
    source_version, is_deleted, silver_updated_at, op, source_snapshot
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.registration_id') AS BIGINT) AS registration_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id')      AS BIGINT) AS vehicle_id,
      get_json_object(coalesce(after_json, before_json), '$.status') AS status,
      timestamp_micros(
        CAST(get_json_object(coalesce(after_json, before_json), '$.updated_at') AS BIGINT)
      ) AS source_updated_at,
      source_lsn           AS source_version,
      (op = 'd')           AS is_deleted,
      current_timestamp()  AS silver_updated_at,
      op,
      source_snapshot,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.registration_id') AS BIGINT)
        ORDER BY source_lsn DESC NULLS LAST
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.registration_id = s.registration_id
WHEN MATCHED AND (
    (s.op = 'r' AND s.source_snapshot = 'incremental')
    OR s.source_version > t.source_version
) THEN UPDATE SET
    vehicle_id        = s.vehicle_id,
    status            = s.status,
    source_updated_at = s.source_updated_at,
    source_version    = COALESCE(s.source_version, t.source_version),
    is_deleted        = s.is_deleted,
    silver_updated_at = s.silver_updated_at
WHEN NOT MATCHED THEN INSERT (
    tenant_id, registration_id, vehicle_id, status, source_updated_at,
    source_version, is_deleted, silver_updated_at
) VALUES (
    s.tenant_id, s.registration_id, s.vehicle_id, s.status, s.source_updated_at,
    COALESCE(s.source_version, 0), s.is_deleted, s.silver_updated_at
)
"""

MERGE_PAYMENT = """
MERGE INTO demo.silver.payment t
USING (
  SELECT
    tenant_id, payment_id, registration_id, amount,
    source_version, is_deleted, silver_updated_at, op, source_snapshot
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.payment_id')      AS BIGINT) AS payment_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.registration_id') AS BIGINT) AS registration_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.amount')          AS DECIMAL(12,2)) AS amount,
      source_lsn           AS source_version,
      (op = 'd')           AS is_deleted,
      current_timestamp()  AS silver_updated_at,
      op,
      source_snapshot,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.payment_id') AS BIGINT)
        ORDER BY source_lsn DESC NULLS LAST
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.payment_id = s.payment_id
WHEN MATCHED AND (
    (s.op = 'r' AND s.source_snapshot = 'incremental')
    OR s.source_version > t.source_version
) THEN UPDATE SET
    registration_id   = s.registration_id,
    amount            = s.amount,
    source_version    = COALESCE(s.source_version, t.source_version),
    is_deleted        = s.is_deleted,
    silver_updated_at = s.silver_updated_at
WHEN NOT MATCHED THEN INSERT (
    tenant_id, payment_id, registration_id, amount,
    source_version, is_deleted, silver_updated_at
) VALUES (
    s.tenant_id, s.payment_id, s.registration_id, s.amount,
    COALESCE(s.source_version, 0), s.is_deleted, s.silver_updated_at
)
"""

STREAMS = [
    ("demo.bronze.vehicle_cdc",      MERGE_VEHICLE,      "silver_vehicle"),
    ("demo.bronze.registration_cdc", MERGE_REGISTRATION, "silver_registration"),
    ("demo.bronze.payment_cdc",      MERGE_PAYMENT,      "silver_payment"),
]


def make_batch_fn(merge_sql: str, view: str):
    def _process(batch_df: DataFrame, batch_id: int):
        if batch_df.isEmpty():
            return
        # Use the batch's own SparkSession — foreachBatch runs in a cloned
        # session, and views are session-scoped.
        session = batch_df.sparkSession
        batch_df.createOrReplaceTempView(view)
        session.sql(merge_sql.format(view=view))
    return _process


def main():
    spark = SparkSession.builder.appName("bronze-to-silver").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for source, merge_sql, name in STREAMS:
        print(f"starting stream: {source} -> {name}")
        view = f"_batch_{name}"
        (
            spark.readStream.format("iceberg").load(source)
            .writeStream
            .foreachBatch(make_batch_fn(merge_sql, view))
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{name}")
            .trigger(processingTime="5 seconds")
            .start()
        )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
