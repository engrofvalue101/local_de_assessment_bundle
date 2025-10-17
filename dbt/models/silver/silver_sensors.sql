-- models/silver/silver_sensors.sql
{{
    config(
        materialized='incremental',
        unique_key=['store_id', 'shelf_id', 'sensor_ts'],
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH sensors_raw AS (
    SELECT * FROM {{ ref('stg_sensors') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

-- Enrichment only (NO aggregation)
sensors_enriched AS (
    SELECT
        sensor_ts,
        CAST(sensor_ts AS DATE) AS sensor_date,
        store_id,
        shelf_id,
        temperature_c,
        humidity_pct,
        battery_mv,
        
        -- Derived: Anomaly flags (but keep detail!)
        CASE 
            WHEN temperature_c < -20 OR temperature_c > 50 THEN TRUE 
            ELSE FALSE 
        END AS has_temperature_anomaly,
        
        CASE 
            WHEN humidity_pct < 10 OR humidity_pct > 95 THEN TRUE 
            ELSE FALSE 
        END AS has_humidity_anomaly,
        
        -- Derived: Battery level category
        CASE
            WHEN battery_mv < 2000 THEN 'Critical'
            WHEN battery_mv >= 2000 AND battery_mv < 2500 THEN 'Low'
            WHEN battery_mv >= 2500 AND battery_mv < 3000 THEN 'Medium'
            WHEN battery_mv >= 3000 THEN 'Good'
            ELSE 'Unknown'
        END AS battery_level,
        
        -- Derived: Temperature range
        CASE
            WHEN temperature_c < 0 THEN 'Below Freezing'
            WHEN temperature_c >= 0 AND temperature_c < 10 THEN 'Cold'
            WHEN temperature_c >= 10 AND temperature_c < 20 THEN 'Cool'
            WHEN temperature_c >= 20 AND temperature_c < 25 THEN 'Optimal'
            WHEN temperature_c >= 25 AND temperature_c < 30 THEN 'Warm'
            WHEN temperature_c >= 30 THEN 'Hot'
            ELSE 'Unknown'
        END AS temperature_range,
        
        -- Derived: Humidity range
        CASE
            WHEN humidity_pct < 30 THEN 'Dry'
            WHEN humidity_pct >= 30 AND humidity_pct < 60 THEN 'Optimal'
            WHEN humidity_pct >= 60 AND humidity_pct < 80 THEN 'Humid'
            WHEN humidity_pct >= 80 THEN 'Very Humid'
            ELSE 'Unknown'
        END AS humidity_range,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM sensors_raw
)

SELECT * FROM sensors_enriched