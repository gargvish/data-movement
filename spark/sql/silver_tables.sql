CREATE NAMESPACE IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.vehicle (
    tenant_id          INT     NOT NULL,
    vehicle_id         BIGINT  NOT NULL,
    vin                STRING,
    make               STRING,
    model              STRING,
    source_version     BIGINT  NOT NULL,   -- Postgres LSN; monotonic version
    is_deleted         BOOLEAN NOT NULL,
    silver_updated_at  TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

CREATE TABLE IF NOT EXISTS silver.registration (
    tenant_id           INT       NOT NULL,
    registration_id     BIGINT    NOT NULL,
    vehicle_id          BIGINT,
    status              STRING,
    source_updated_at   TIMESTAMP,
    source_version      BIGINT    NOT NULL,
    is_deleted          BOOLEAN   NOT NULL,
    silver_updated_at   TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

CREATE TABLE IF NOT EXISTS silver.payment (
    tenant_id          INT            NOT NULL,
    payment_id         BIGINT         NOT NULL,
    registration_id    BIGINT,
    amount             DECIMAL(12,2),
    source_version     BIGINT         NOT NULL,
    is_deleted         BOOLEAN        NOT NULL,
    silver_updated_at  TIMESTAMP      NOT NULL
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);
