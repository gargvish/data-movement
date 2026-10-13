-- Demo 4 / schema evolution.
-- Mirror the Postgres migration into Silver.
--
-- No DEFAULT: Silver is derived from Bronze/CDC — the value for each row
-- comes from the stream, not from a Silver-side default. Pre-ALTER rows
-- stay NULL until the matching Postgres UPDATE arrives via CDC (which,
-- in the companion migration 001_vehicle_emission_class.sql, we trigger
-- explicitly with a bulk UPDATE).

ALTER TABLE silver.vehicle ADD COLUMN emission_class STRING;
