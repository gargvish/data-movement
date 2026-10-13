-- Tenants (small lookup; agencies that own vehicle registrations)
CREATE TABLE tenant (
    tenant_id       INT PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,
    classification  VARCHAR(20)  NOT NULL CHECK (classification IN ('whale', 'shared'))
);

INSERT INTO tenant (tenant_id, name, classification) VALUES
    (101, 'California DMV',   'whale'),
    (102, 'Nevada DMV',       'shared'),
    (103, 'Rhode Island DMV', 'shared');

-- Operational tables (composite PKs keyed on tenant_id)
CREATE TABLE vehicle (
    tenant_id   INT         NOT NULL,
    vehicle_id  BIGINT      NOT NULL,
    vin         VARCHAR(17),
    make        VARCHAR(50),
    model       VARCHAR(50),
    PRIMARY KEY (tenant_id, vehicle_id)
);

CREATE TABLE registration (
    tenant_id        INT         NOT NULL,
    registration_id  BIGINT      NOT NULL,
    vehicle_id       BIGINT      NOT NULL,
    status           VARCHAR(30),
    updated_at       TIMESTAMP   NOT NULL DEFAULT now(),
    PRIMARY KEY (tenant_id, registration_id)
);

CREATE TABLE payment (
    tenant_id        INT             NOT NULL,
    payment_id       BIGINT          NOT NULL,
    registration_id  BIGINT          NOT NULL,
    amount           DECIMAL(12, 2),
    PRIMARY KEY (tenant_id, payment_id)
);

-- Full before-images on UPDATE/DELETE so Debezium emits complete "before" payloads.
-- Composite PKs with REPLICA IDENTITY DEFAULT would only log PK columns.
ALTER TABLE vehicle      REPLICA IDENTITY FULL;
ALTER TABLE registration REPLICA IDENTITY FULL;
ALTER TABLE payment      REPLICA IDENTITY FULL;

-- Debezium replication user
CREATE ROLE debezium WITH REPLICATION LOGIN PASSWORD 'debezium';
GRANT CONNECT ON DATABASE dmv TO debezium;
GRANT USAGE ON SCHEMA public TO debezium;
GRANT SELECT ON vehicle, registration, payment, tenant TO debezium;

-- Publication the Debezium connector will subscribe to
CREATE PUBLICATION dbz_pub FOR TABLE vehicle, registration, payment;
