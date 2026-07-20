-- Lesson 03 — Tier 2: Advanced SQL — Recursive CTEs, Anti-Joins

-- Schema-on-write: create typed Bronze table
CREATE SCHEMA IF NOT EXISTS bronze;

CREATE OR REPLACE TABLE bronze.yellow_trips AS
SELECT
    VendorID                                        AS vendor_id,
    tpep_pickup_datetime::TIMESTAMP                 AS pickup_datetime,
    tpep_dropoff_datetime::TIMESTAMP                AS dropoff_datetime,
    passenger_count::SMALLINT                       AS passenger_count,
    trip_distance::DOUBLE                           AS trip_distance,
    RatecodeID::SMALLINT                            AS rate_code_id,
    PULocationID::INTEGER                           AS pu_location_id,
    DOLocationID::INTEGER                           AS do_location_id,
    payment_type::SMALLINT                          AS payment_type,
    fare_amount::DECIMAL(10,2)                      AS fare_amount,
    extra::DECIMAL(10,2)                            AS extra,
    mta_tax::DECIMAL(10,2)                          AS mta_tax,
    tip_amount::DECIMAL(10,2)                       AS tip_amount,
    tolls_amount::DECIMAL(10,2)                     AS tolls_amount,
    improvement_surcharge::DECIMAL(10,2)            AS improvement_surcharge,
    total_amount::DECIMAL(10,2)                     AS total_amount,
    congestion_surcharge::DECIMAL(10,2)             AS congestion_surcharge
FROM 'data/landing/yellow_tripdata_2024-01.parquet';

-- Recursive CTE: generate calendar dimension for January 2024
WITH RECURSIVE calendar(day_date) AS (
    SELECT DATE '2024-01-01'
    UNION ALL
    SELECT day_date + INTERVAL '1 day'
    FROM calendar
    WHERE day_date < DATE '2024-01-31'
)
SELECT
    day_date,
    EXTRACT(DOW FROM day_date)  AS day_of_week,
    DAYNAME(day_date)           AS day_name,
    CASE WHEN EXTRACT(DOW FROM day_date) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM calendar;

-- Anti-join: find zones with no pickup in January 2024
-- (uses taxi_zone_lookup as reference)
SELECT z.LocationID, z.Zone, z.Borough
FROM 'data/reference/taxi_zone_lookup.csv' z
WHERE NOT EXISTS (
    SELECT 1
    FROM bronze.yellow_trips t
    WHERE t.pu_location_id = z.LocationID
)
ORDER BY z.Borough, z.Zone;

-- Chained CTEs: hourly revenue by zone, top-3 zones per hour
WITH hourly_zone AS (
    SELECT
        EXTRACT(HOUR FROM pickup_datetime)  AS pickup_hour,
        pu_location_id,
        SUM(fare_amount)                    AS total_fare,
        COUNT(*)                            AS trip_count
    FROM bronze.yellow_trips
    WHERE fare_amount > 0
    GROUP BY 1, 2
),
ranked AS (
    SELECT
        pickup_hour,
        pu_location_id,
        total_fare,
        trip_count,
        ROW_NUMBER() OVER (PARTITION BY pickup_hour ORDER BY total_fare DESC) AS rn
    FROM hourly_zone
)
SELECT pickup_hour, pu_location_id, total_fare, trip_count
FROM ranked
WHERE rn <= 3
ORDER BY pickup_hour, rn;
