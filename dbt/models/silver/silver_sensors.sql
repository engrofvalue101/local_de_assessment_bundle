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
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(ingestion_ts), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),
-- Enrichment only (NO aggregation)
sensors_with_validation AS (
    SELECT
        *,
        
        -- VALIDATION FLAGS (based on staging tests and business rules)
        CASE WHEN sensor_ts IS NULL THEN TRUE ELSE FALSE END AS has_null_sensor_ts,
        CASE WHEN store_id IS NULL THEN TRUE ELSE FALSE END AS has_null_store_id,
        CASE WHEN shelf_id IS NULL THEN TRUE ELSE FALSE END AS has_null_shelf_id,
        
        -- Relationship check
        CASE WHEN store_id IS NOT NULL THEN FALSE ELSE FALSE END AS has_invalid_store_relationship,
        
        -- Anomaly checks (extreme values that indicate sensor malfunction)
        CASE 
            WHEN temperature_c IS NOT NULL AND (temperature_c < -50 OR temperature_c > 80)
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_temperature_anomaly,
        
        CASE 
            WHEN humidity_pct IS NOT NULL AND (humidity_pct < 0 OR humidity_pct > 100)
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_humidity_anomaly,
        
        CASE 
            WHEN battery_mv IS NOT NULL AND (battery_mv < 0 OR battery_mv > 5000)
            THEN TRUE 
            ELSE FALSE 
        END AS has_extreme_battery_anomaly,
        
        -- Future timestamp check
        CASE WHEN sensor_ts > CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END AS has_future_sensor_ts,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN sensor_ts IS NULL THEN FALSE
            WHEN store_id IS NULL THEN FALSE
            WHEN shelf_id IS NULL THEN FALSE
            WHEN temperature_c IS NOT NULL AND (temperature_c < -50 OR temperature_c > 80) THEN FALSE
            WHEN humidity_pct IS NOT NULL AND (humidity_pct < 0 OR humidity_pct > 100) THEN FALSE
            WHEN battery_mv IS NOT NULL AND (battery_mv < 0 OR battery_mv > 5000) THEN FALSE
            WHEN sensor_ts > CURRENT_TIMESTAMP THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN sensor_ts IS NULL THEN 'Missing Sensor Timestamp'
            WHEN store_id IS NULL THEN 'Missing Store ID'
            WHEN shelf_id IS NULL THEN 'Missing Shelf ID'
            WHEN temperature_c IS NOT NULL AND (temperature_c < -50 OR temperature_c > 80) 
                THEN 'Extreme Temperature Anomaly'
            WHEN humidity_pct IS NOT NULL AND (humidity_pct < 0 OR humidity_pct > 100) 
                THEN 'Extreme Humidity Anomaly'
            WHEN battery_mv IS NOT NULL AND (battery_mv < 0 OR battery_mv > 5000) 
                THEN 'Extreme Battery Anomaly'
            WHEN sensor_ts > CURRENT_TIMESTAMP THEN 'Future Sensor Timestamp'
            ELSE NULL
        END AS quality_issue_type
        
    FROM sensors_raw
),

-- Enrichment
sensors_enriched AS (
    SELECT
        sensor_ts,
        CAST(sensor_ts AS DATE) AS sensor_date,
        store_id,
        shelf_id,
        temperature_c,
        humidity_pct,
        battery_mv,
        
        -- Quality flags
        has_null_sensor_ts,
        has_null_store_id,
        has_null_shelf_id,
        has_invalid_store_relationship,
        has_extreme_temperature_anomaly,
        has_extreme_humidity_anomaly,
        has_extreme_battery_anomaly,
        has_future_sensor_ts,
        is_valid_record,
        quality_issue_type,
        
        -- Operational anomaly flags (less severe, for monitoring)
        CASE 
            WHEN temperature_c IS NOT NULL AND (temperature_c < -20 OR temperature_c > 50)
            THEN TRUE 
            ELSE FALSE 
        END AS has_temperature_anomaly,
        
        CASE 
            WHEN humidity_pct IS NOT NULL AND (humidity_pct < 10 OR humidity_pct > 95)
            THEN TRUE 
            ELSE FALSE 
        END AS has_humidity_anomaly,
        
        -- Derived fields
        CASE
            WHEN battery_mv < 2000 THEN 'Critical'
            WHEN battery_mv >= 2000 AND battery_mv < 2500 THEN 'Low'
            WHEN battery_mv >= 2500 AND battery_mv < 3000 THEN 'Medium'
            WHEN battery_mv >= 3000 THEN 'Good'
            ELSE 'Unknown'
        END AS battery_level,
        
        CASE
            WHEN temperature_c < 0 THEN 'Below Freezing'
            WHEN temperature_c >= 0 AND temperature_c < 10 THEN 'Cold'
            WHEN temperature_c >= 10 AND temperature_c < 20 THEN 'Cool'
            WHEN temperature_c >= 20 AND temperature_c < 25 THEN 'Optimal'
            WHEN temperature_c >= 25 AND temperature_c < 30 THEN 'Warm'
            WHEN temperature_c >= 30 THEN 'Hot'
            ELSE 'Unknown'
        END AS temperature_range,
        
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
        
    FROM sensors_with_validation
)

SELECT * FROM sensors_enriched