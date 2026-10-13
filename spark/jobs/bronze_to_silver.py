"""
Bronze -> Silver: current-state tables maintained via MERGE INTO.

One Structured Streaming query per table. Each micro-batch:
  1. Reads newly-appended rows from bronze.<table>_cdc.
  2. Within the batch, keeps the row with max source_lsn per primary key.
  3. MERGE INTO silver.<table>, applying the change only when
     incoming.source_lsn > silver.source_version (idempotent on replay).
Deletes are soft: is_deleted=true with bumped source_version.
"""
from pyspark.sql import SparkSession, DataFrame

CHECKPOINT_BASE = "s3a://warehouse/_checkpoints"


# Each MERGE below:
#   - reads from a temp view {view} that holds one micro-batch of bronze rows
#   - uses coalesce(after_json, before_json) to resolve the PK (after is null on delete)
#   - deduplicates with row_number() over source_lsn DESC
#   - applies UPDATE only when source_lsn beats stored source_version

MERGE_VEHICLE = """
MERGE INTO demo.silver.vehicle t
USING (
  SELECT
    tenant_id, vehicle_id, vin, make, model,
    source_version, is_deleted, silver_updated_at
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT) AS vehicle_id,
      get_json_object(coalesce(after_json, before_json), '$.vin')   AS vin,
      get_json_object(coalesce(after_json, before_json), '$.make')  AS make,
      get_json_object(coalesce(after_json, before_json), '$.model') AS model,
      source_lsn           AS source_version,
      (op = 'd')           AS is_deleted,
      current_timestamp()  AS silver_updated_at,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.vehicle_id') AS BIGINT)
        ORDER BY source_lsn DESC
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.vehicle_id = s.vehicle_id
WHEN MATCHED AND s.source_version > t.source_version THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

MERGE_REGISTRATION = """
MERGE INTO demo.silver.registration t
USING (
  SELECT
    tenant_id, registration_id, vehicle_id, status, source_updated_at,
    source_version, is_deleted, silver_updated_at
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
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.registration_id') AS BIGINT)
        ORDER BY source_lsn DESC
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.registration_id = s.registration_id
WHEN MATCHED AND s.source_version > t.source_version THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

MERGE_PAYMENT = """
MERGE INTO demo.silver.payment t
USING (
  SELECT
    tenant_id, payment_id, registration_id, amount,
    source_version, is_deleted, silver_updated_at
  FROM (
    SELECT
      tenant_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.payment_id')      AS BIGINT) AS payment_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.registration_id') AS BIGINT) AS registration_id,
      CAST(get_json_object(coalesce(after_json, before_json), '$.amount')          AS DECIMAL(12,2)) AS amount,
      source_lsn           AS source_version,
      (op = 'd')           AS is_deleted,
      current_timestamp()  AS silver_updated_at,
      row_number() OVER (
        PARTITION BY tenant_id,
                     CAST(get_json_object(coalesce(after_json, before_json), '$.payment_id') AS BIGINT)
        ORDER BY source_lsn DESC
      ) AS rn
    FROM {view}
  )
  WHERE rn = 1
) s
ON t.tenant_id = s.tenant_id AND t.payment_id = s.payment_id
WHEN MATCHED AND s.source_version > t.source_version THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
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
