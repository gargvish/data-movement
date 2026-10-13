-- Demo 5 / metadata hygiene.
-- Enable automatic pruning of old metadata.json files on each commit.
-- Without this, every commit leaves its old metadata.json on disk forever.
-- expire_snapshots and remove_orphan_files do not touch metadata.json files;
-- only this table property does, as a side effect of each new commit.
--
-- previous-versions-max = 3 keeps the most recent 3 metadata.json files for
-- a small amount of pointer history (used by catalogs that occasionally need
-- to recover from a stale pointer). Set higher if you want more lookback.

ALTER TABLE bronze.vehicle_cdc      SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE bronze.registration_cdc SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE bronze.payment_cdc      SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');

ALTER TABLE silver.vehicle          SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE silver.registration     SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE silver.payment          SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');

ALTER TABLE gold.daily_registrations SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE gold.tenant_revenue      SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
ALTER TABLE gold.active_vehicles     SET TBLPROPERTIES ('write.metadata.delete-after-commit.enabled' = 'true', 'write.metadata.previous-versions-max' = '3');
