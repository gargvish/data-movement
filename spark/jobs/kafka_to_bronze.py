"""
Debezium CDC on db.public.<table> topics -> bronze.<table>_cdc Iceberg tables.
One streaming query per table so each has its own checkpoint and can be
restarted independently.
"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object, current_timestamp, coalesce

KAFKA_BOOTSTRAP = "kafka:29092"
CHECKPOINT_BASE = "s3a://warehouse/_checkpoints"

TABLES = [
    ("db.public.vehicle",      "bronze.vehicle_cdc"),
    ("db.public.registration", "bronze.registration_cdc"),
    ("db.public.payment",      "bronze.payment_cdc"),
]


def start_query(spark: SparkSession, topic: str, table: str):
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    # Debezium emits a tombstone (value=null) after every delete. Filter them;
    # the preceding op=d event already carries the full before-image.
    envelope = (
        raw
        .filter(col("value").isNotNull())
        .selectExpr(
            "CAST(value AS STRING) AS envelope_json",
            "topic AS kafka_topic",
            "partition AS kafka_partition",
            "offset AS kafka_offset",
        )
    )

    parsed = envelope.select(
        get_json_object(col("envelope_json"), "$.op").alias("op"),
        get_json_object(col("envelope_json"), "$.before").alias("before_json"),
        get_json_object(col("envelope_json"), "$.after").alias("after_json"),
        # tenant_id lives in `after` for inserts/updates/snapshots and in
        # `before` for deletes. Coalesce picks whichever is non-null.
        coalesce(
            get_json_object(col("envelope_json"), "$.after.tenant_id"),
            get_json_object(col("envelope_json"), "$.before.tenant_id"),
        ).cast("int").alias("tenant_id"),
        get_json_object(col("envelope_json"), "$.source.lsn").cast("bigint").alias("source_lsn"),
        get_json_object(col("envelope_json"), "$.source.txId").cast("bigint").alias("source_txid"),
        get_json_object(col("envelope_json"), "$.source.ts_ms").cast("bigint").alias("source_ts_ms"),
        get_json_object(col("envelope_json"), "$.source.snapshot").alias("source_snapshot"),
        current_timestamp().alias("ingest_ts"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
    )

    return (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table.replace('.', '_')}")
        .trigger(processingTime="5 seconds")
        .toTable(table)
    )


def main():
    spark = (
        SparkSession.builder
        .appName("kafka-to-bronze")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    for topic, table in TABLES:
        print(f"starting stream: {topic} -> {table}")
        start_query(spark, topic, table)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
