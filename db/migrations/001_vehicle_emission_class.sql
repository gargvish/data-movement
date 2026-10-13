-- Demo 4 / schema evolution.
-- Add emission_class to vehicle.
--
-- Rationale for DEFAULT: in Postgres 11+ this is a metadata-only operation
-- (uses attmissingval) so no table rewrite and no row-level WAL. New rows
-- get 'UNKNOWN' automatically; pre-ALTER rows return 'UNKNOWN' on SELECT.
--
-- Rationale for the UPDATE: SELECT returning 'UNKNOWN' via attmissingval is
-- a Postgres-side read-time resolution; those rows are still physically NULL.
-- CDC fires on physical row changes, so without this UPDATE Silver would see
-- NULL for every pre-ALTER row until it's next modified. The UPDATE triggers
-- one CDC event per row and lets the normal stream fill Silver.

ALTER TABLE vehicle ADD COLUMN emission_class VARCHAR(20) DEFAULT 'UNKNOWN';
UPDATE vehicle SET emission_class = 'UNKNOWN' WHERE emission_class IS NULL;
