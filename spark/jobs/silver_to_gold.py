"""
Silver + Bronze -> Gold aggregations. One-shot batch job: three
INSERT OVERWRITE queries, then exit. Safe to re-run (deterministic from
current Silver/Bronze state).
"""
from pyspark.sql import SparkSession


# gold.daily_registrations: event counts per (tenant, day).
# Reads Bronze — Silver has no history, we need to count when each change
# happened. Dates come from Postgres commit time (source.ts_ms).
DAILY_REGISTRATIONS = """
INSERT OVERWRITE demo.gold.daily_registrations
SELECT
    event_date,
    tenant_id,
    sum(CASE WHEN op = 'c' THEN 1 ELSE 0 END) AS new_registrations,
    sum(CASE WHEN op = 'u' AND after_status = 'SUSPENDED' THEN 1 ELSE 0 END) AS suspensions,
    current_timestamp() AS computed_at
FROM (
    SELECT
        to_date(timestamp_millis(source_ts_ms)) AS event_date,
        tenant_id,
        op,
        get_json_object(after_json, '$.status') AS after_status
    FROM demo.bronze.registration_cdc
    WHERE op IN ('c', 'u')      -- snapshot events (r) and deletes (d) don't count here
)
GROUP BY event_date, tenant_id
"""


# gold.tenant_revenue: live revenue per tenant.
# Live payment + live registration — both must not be soft-deleted.
TENANT_REVENUE = """
INSERT OVERWRITE demo.gold.tenant_revenue
SELECT
    p.tenant_id,
    sum(p.amount) AS total_revenue,
    count(*)      AS payment_count,
    current_timestamp() AS computed_at
FROM demo.silver.payment p
JOIN demo.silver.registration r
  ON p.tenant_id = r.tenant_id AND p.registration_id = r.registration_id
WHERE p.is_deleted = false
  AND r.is_deleted = false
  AND p.amount IS NOT NULL
GROUP BY p.tenant_id
"""


# gold.active_vehicles: one row per currently-active vehicle registration.
# "Active" = all three of: vehicle not deleted, registration not deleted,
# registration.status = 'ACTIVE'.
ACTIVE_VEHICLES = """
INSERT OVERWRITE demo.gold.active_vehicles
SELECT
    v.tenant_id,
    v.vehicle_id,
    v.vin,
    v.make,
    v.model,
    r.registration_id,
    r.status AS registration_status,
    r.source_updated_at,
    current_timestamp() AS computed_at
FROM demo.silver.vehicle v
JOIN demo.silver.registration r
  ON v.tenant_id = r.tenant_id AND v.vehicle_id = r.vehicle_id
WHERE v.is_deleted = false
  AND r.is_deleted = false
  AND r.status = 'ACTIVE'
"""


QUERIES = [
    ("gold.daily_registrations", DAILY_REGISTRATIONS),
    ("gold.tenant_revenue",      TENANT_REVENUE),
    ("gold.active_vehicles",     ACTIVE_VEHICLES),
]


def main():
    spark = SparkSession.builder.appName("silver-to-gold").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    for name, sql in QUERIES:
        print(f"rebuilding {name}")
        spark.sql(sql)

    print("done")
    spark.stop()


if __name__ == "__main__":
    main()
