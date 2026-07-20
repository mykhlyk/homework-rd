-- Lesson 03 — Tier 1: DuckDB SQL on NYC Taxi
-- Run with: duckdb taxi_dwh.duckdb < tier1_queries.sql

-- Schema-on-read: query raw Parquet without CREATE TABLE
SELECT COUNT(*) AS total_trips
FROM 'data/landing/yellow_tripdata_2024-01.parquet';

-- Basic aggregation
SELECT
    PULocationID,
    COUNT(*)                        AS trip_count,
    ROUND(AVG(fare_amount), 2)      AS avg_fare,
    ROUND(SUM(fare_amount), 0)      AS total_fare,
    ROUND(AVG(trip_distance), 2)    AS avg_distance
FROM 'data/landing/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 0
GROUP BY PULocationID
ORDER BY trip_count DESC
LIMIT 10;

-- Window function: rank zones by revenue
SELECT
    PULocationID,
    SUM(fare_amount)                                          AS total_fare,
    RANK() OVER (ORDER BY SUM(fare_amount) DESC)             AS revenue_rank,
    DENSE_RANK() OVER (ORDER BY SUM(fare_amount) DESC)       AS dense_rank,
    NTILE(4) OVER (ORDER BY SUM(fare_amount) DESC)           AS quartile
FROM 'data/landing/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 0
GROUP BY PULocationID
ORDER BY revenue_rank;

-- LAG/LEAD: daily trip count with previous day comparison
WITH daily AS (
    SELECT
        DATE_TRUNC('day', tpep_pickup_datetime) AS pickup_day,
        COUNT(*)                                AS trip_count
    FROM 'data/landing/yellow_tripdata_2024-01.parquet'
    GROUP BY 1
)
SELECT
    pickup_day,
    trip_count,
    LAG(trip_count)  OVER (ORDER BY pickup_day) AS prev_day_count,
    LEAD(trip_count) OVER (ORDER BY pickup_day) AS next_day_count,
    trip_count - LAG(trip_count) OVER (ORDER BY pickup_day) AS day_over_day_delta
FROM daily
ORDER BY pickup_day;

-- Running total per zone
SELECT
    PULocationID,
    tpep_pickup_datetime,
    fare_amount,
    SUM(fare_amount) OVER (
        PARTITION BY PULocationID
        ORDER BY tpep_pickup_datetime
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_fare_per_zone
FROM 'data/landing/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 0
  AND PULocationID = 237
ORDER BY tpep_pickup_datetime
LIMIT 20;

-- QUALIFY: top-1 trip per zone (DuckDB / BigQuery / Snowflake syntax)
SELECT *
FROM 'data/landing/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 0
QUALIFY ROW_NUMBER() OVER (PARTITION BY PULocationID ORDER BY fare_amount DESC) = 1
ORDER BY PULocationID
LIMIT 10;

-- EXPLAIN: see query plan
EXPLAIN
SELECT PULocationID, COUNT(*), AVG(fare_amount)
FROM 'data/landing/yellow_tripdata_2024-01.parquet'
WHERE fare_amount > 0
GROUP BY PULocationID;
