-- models/gold/facts/fct_sensor_readings.sql
{{
    config(
        materialized='incremental',
        unique_key='sensor_reading_key',
        on_schema_change='merge',
        schema='gold'
    )
}}

WITH silver_sensors AS (
    SELECT * FROM {{ ref('silver_sensors') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

dim_store AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
),

dim_date AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

-- Pre-aggregate to hourly level
hourly_aggregated AS (
    SELECT
        store_id,
        DATE_TRUNC('hour', sensor_ts) AS reading_hour,
        DATE_TRUNC('day', sensor_ts)::DATE AS reading_date,
        
        -- Temperature metrics
        AVG(temperature_c) AS avg_temperature_c,
        MIN(temperature_c) AS min_temperature_c,
        MAX(temperature_c) AS max_temperature_c,
        STDDEV(temperature_c) AS stddev_temperature_c,
        
        -- Humidity metrics
        AVG(humidity_pct) AS avg_humidity_pct,
        MIN(humidity_pct) AS min_humidity_pct,
        MAX(humidity_pct) AS max_humidity_pct,
        STDDEV(humidity_pct) AS stddev_humidity_pct,
        
        -- Battery metrics
        AVG(battery_mv) AS avg_battery_mv,
        MIN(battery_mv) AS min_battery_mv,
        
        -- Data quality
        COUNT(*) AS reading_count,
        COUNT(*) FILTER (WHERE temperature_c IS NOT NULL) AS valid_temp_readings,
        COUNT(*) FILTER (WHERE humidity_pct IS NOT NULL) AS valid_humidity_readings,
        
        -- Count anomalies from silver layer
        COUNT(*) FILTER (WHERE has_temperature_anomaly = TRUE) AS temp_anomaly_count,
        COUNT(*) FILTER (WHERE has_humidity_anomaly = TRUE) AS humidity_anomaly_count,  
        
        -- Latest ingestion timestamp for this hour
        MAX(ingestion_ts) AS max_ingestion_ts
        
    FROM silver_sensors
    GROUP BY 
        store_id,
        DATE_TRUNC('hour', sensor_ts),
        DATE_TRUNC('day', sensor_ts)::DATE
),

with_anomaly_flags AS (
    SELECT
        *,
        
        -- Temperature anomaly flags (normal range 15-30°C)
        CASE
            WHEN avg_temperature_c < 15 THEN TRUE
            WHEN avg_temperature_c > 30 THEN TRUE
            ELSE FALSE
        END AS temp_out_of_range,
        
        CASE
            WHEN max_temperature_c - min_temperature_c > 10 THEN TRUE
            ELSE FALSE
        END AS temp_high_variance,
        
        -- Humidity anomaly flags (normal range 30-70%)
        CASE
            WHEN avg_humidity_pct < 30 THEN TRUE
            WHEN avg_humidity_pct > 70 THEN TRUE
            ELSE FALSE
        END AS humidity_out_of_range,
        
        CASE
            WHEN max_humidity_pct - min_humidity_pct > 30 THEN TRUE
            ELSE FALSE
        END AS humidity_high_variance,
        
        -- Battery anomaly (low battery < 2500mV)
        CASE
            WHEN min_battery_mv < 2500 THEN TRUE
            ELSE FALSE
        END AS low_battery,
        
        -- Overall anomaly flag
        CASE
            WHEN avg_temperature_c < 15 OR avg_temperature_c > 30 THEN TRUE
            WHEN avg_humidity_pct < 30 OR avg_humidity_pct > 70 THEN TRUE
            WHEN max_temperature_c - min_temperature_c > 10 THEN TRUE
            WHEN max_humidity_pct - min_humidity_pct > 30 THEN TRUE
            WHEN min_battery_mv < 2500 THEN TRUE
            ELSE FALSE
        END AS has_anomaly
        
    FROM hourly_aggregated
),

sensors_with_keys AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['s.store_id', 's.reading_hour']) }} AS sensor_reading_key,
        
        -- Foreign keys
        st.store_key,
        d.date_key,
        
        -- Degenerate dimensions
        s.store_id,
        s.reading_hour,
        s.reading_date,
        EXTRACT(HOUR FROM s.reading_hour) AS reading_hour_of_day,
        
        -- Temperature measures
        s.avg_temperature_c,
        s.min_temperature_c,
        s.max_temperature_c,
        s.stddev_temperature_c,
        
        -- Humidity measures
        s.avg_humidity_pct,
        s.min_humidity_pct,
        s.max_humidity_pct,
        s.stddev_humidity_pct,
        
        -- Battery measures
        s.avg_battery_mv,
        s.min_battery_mv,
        
        -- Data quality measures
        s.reading_count,
        s.valid_temp_readings,
        s.valid_humidity_readings,
        s.valid_temp_readings::FLOAT / s.reading_count AS temp_data_quality_pct,
        s.valid_humidity_readings::FLOAT / s.reading_count AS humidity_data_quality_pct,
        
        -- Anomaly counts
        s.temp_anomaly_count,
        s.humidity_anomaly_count,
        
        -- Anomaly flags
        s.temp_out_of_range,
        s.temp_high_variance,
        s.humidity_out_of_range,
        s.humidity_high_variance,
        s.low_battery,
        s.has_anomaly,
        
        -- Audit
        s.max_ingestion_ts AS ingestion_ts,
        CURRENT_TIMESTAMP AS fact_created_at
        
    FROM with_anomaly_flags s
    INNER JOIN dim_store st ON s.store_id = st.store_id
    INNER JOIN dim_date d ON s.reading_date = d.date_day
)

SELECT * FROM sensors_with_keys
{% if is_incremental() %}
    WHERE sensor_reading_key NOT IN (SELECT sensor_reading_key FROM {{ this }})
{% endif %}