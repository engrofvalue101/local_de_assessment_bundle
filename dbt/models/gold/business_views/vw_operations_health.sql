-- models/gold/business_views/vw_operations_health.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH fct_sensors AS (
    SELECT * FROM {{ ref('fct_sensor_readings') }}
),

dim_store AS (
    SELECT 
        store_key,
        store_id,
        store_name,
        region,
        state
    FROM {{ ref('dim_stores') }}
),

dim_date AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
)

SELECT
    -- Store identification
    st.store_id,
    st.store_name,
    st.region,
    st.state,
    
    -- Reading information
    d.date_day AS reading_date,
    s.reading_hour_of_day AS hour_of_day,
    
    -- Temperature metrics (Celsius)
    ROUND(s.avg_temperature_c, 1) AS average_temperature,
    ROUND(s.min_temperature_c, 1) AS minimum_temperature,
    ROUND(s.max_temperature_c, 1) AS maximum_temperature,
    --s.temperature_range,
    
    -- Humidity metrics (Percentage)
    ROUND(s.avg_humidity_pct, 1) AS average_humidity,
    ROUND(s.min_humidity_pct, 1) AS minimum_humidity,
    ROUND(s.max_humidity_pct, 1) AS maximum_humidity,
    --s.humidity_range,
    
    -- Battery health
    s.avg_battery_mv AS average_battery_level,
    --s.battery_level,
    
    -- Data quality
    s.reading_count AS total_readings,
    ROUND(s.temp_data_quality_pct * 100, 1) AS temperature_data_quality_percent,
    ROUND(s.humidity_data_quality_pct * 100, 1) AS humidity_data_quality_percent,
    
    -- Anomaly indicators
    s.temp_anomaly_count AS temperature_anomalies,
    s.humidity_anomaly_count AS humidity_anomalies,
    s.has_anomaly AS has_any_anomaly,
    
    -- Health status
    CASE
        WHEN s.has_anomaly = TRUE THEN 'Issue Detected'
        WHEN s.low_battery = TRUE THEN 'Battery Warning'
        ELSE 'Healthy'
    END AS operational_status,
    
    -- Alert priority
    CASE
        WHEN s.temp_out_of_range = TRUE OR s.humidity_out_of_range = TRUE THEN 'High Priority'
        WHEN s.temp_high_variance = TRUE OR s.humidity_high_variance = TRUE THEN 'Medium Priority'
        WHEN s.low_battery = TRUE THEN 'Low Priority'
        ELSE 'No Alert'
    END AS alert_priority

FROM fct_sensor_readings s
INNER JOIN dim_store st ON s.store_key = st.store_key
INNER JOIN dim_date d ON s.date_key = d.date_key