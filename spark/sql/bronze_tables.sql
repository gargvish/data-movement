CREATE NAMESPACE IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.vehicle_cdc (
    op              STRING,
    before_json     STRING,
    after_json      STRING,
    tenant_id       INT,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    source_ts_ms    BIGINT,
    source_snapshot STRING,
    ingest_ts       TIMESTAMP,
    kafka_topic     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

CREATE TABLE IF NOT EXISTS bronze.registration_cdc (
    op              STRING,
    before_json     STRING,
    after_json      STRING,
    tenant_id       INT,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    source_ts_ms    BIGINT,
    source_snapshot STRING,
    ingest_ts       TIMESTAMP,
    kafka_topic     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

CREATE TABLE IF NOT EXISTS bronze.payment_cdc (
    op              STRING,
    before_json     STRING,
    after_json      STRING,
    tenant_id       INT,
    source_lsn      BIGINT,
    source_txid     BIGINT,
    source_ts_ms    BIGINT,
    source_snapshot STRING,
    ingest_ts       TIMESTAMP,
    kafka_topic     STRING,
    kafka_partition INT,
    kafka_offset    BIGINT
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);
