CREATE NAMESPACE IF NOT EXISTS gold;

-- Daily event counts per tenant. Computed from bronze.registration_cdc
-- because Silver has no history of when things happened.
CREATE TABLE IF NOT EXISTS gold.daily_registrations (
    event_date         DATE      NOT NULL,
    tenant_id          INT       NOT NULL,
    new_registrations  BIGINT    NOT NULL,
    suspensions        BIGINT    NOT NULL,
    computed_at        TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

-- Current revenue per tenant from live (non-deleted) payments linked
-- to live registrations.
CREATE TABLE IF NOT EXISTS gold.tenant_revenue (
    tenant_id       INT             NOT NULL,
    total_revenue   DECIMAL(14, 2)  NOT NULL,
    payment_count   BIGINT          NOT NULL,
    computed_at     TIMESTAMP       NOT NULL
)
USING iceberg
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);

-- One row per currently-active vehicle (vehicle not deleted + registration
-- status ACTIVE + registration not deleted). Dimension-style snapshot.
CREATE TABLE IF NOT EXISTS gold.active_vehicles (
    tenant_id            INT       NOT NULL,
    vehicle_id           BIGINT    NOT NULL,
    vin                  STRING,
    make                 STRING,
    model                STRING,
    registration_id      BIGINT    NOT NULL,
    registration_status  STRING    NOT NULL,
    source_updated_at    TIMESTAMP,
    computed_at          TIMESTAMP NOT NULL
)
USING iceberg
PARTITIONED BY (tenant_id)
TBLPROPERTIES (
    'write.metadata.delete-after-commit.enabled' = 'true',
    'write.metadata.previous-versions-max'       = '3'
);
