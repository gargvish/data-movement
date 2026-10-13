-- Separate database for the Iceberg REST catalog's JDBC backend.
-- Lives inside dm-postgres but logically isolated from the operational `dmv` DB.

CREATE USER iceberg WITH PASSWORD 'iceberg';
CREATE DATABASE iceberg_catalog OWNER iceberg;
GRANT ALL PRIVILEGES ON DATABASE iceberg_catalog TO iceberg;
