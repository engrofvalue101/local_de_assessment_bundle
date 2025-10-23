-- models/gold/business_views/vw_data_quality_dashboard.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

WITH fct_audit AS (
    SELECT * FROM {{ ref('fct_ingestion_audit') }}
),

dim_date AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
)

SELECT
    -- Time dimension
    d.date_day AS ingestion_date,
    a.ingestion_hour AS hour,
    
    -- Table information
    a.table_name AS data_table,
    a.src_filename AS source_file,
    
    -- Volume metrics
    a.rows_processed,
    a.rows_rejected,
    a.rows_accepted,
    ROUND(a.file_size_mb, 2) AS file_size_mb,
    
    -- Performance metrics
    a.processing_time_seconds AS processing_seconds,
    ROUND(a.rows_per_second, 0) AS throughput_rows_per_second,
    ROUND(a.mb_per_second, 2) AS throughput_mb_per_second,
    
    -- Quality metrics
    ROUND(a.reject_rate_pct, 2) AS rejection_rate_percent,
    a.data_quality_tier AS quality_rating,
    
    -- Status
    a.processing_status AS status,
    a.processing_speed_tier AS speed_rating,
    
    -- Flags
    a.is_success AS successful_load,
    a.has_rejects AS contains_rejections,
    a.is_empty AS empty_file,
    
    -- Health indicators
    CASE
        WHEN a.is_empty = TRUE THEN 'Empty File'
        WHEN a.reject_rate_pct > 10 THEN 'Poor Quality'
        WHEN a.reject_rate_pct > 5 THEN 'Quality Warning'
        WHEN a.processing_speed_tier = 'Slow' THEN 'Performance Warning'
        ELSE 'Healthy'
    END AS pipeline_health

FROM fct_audit a
INNER JOIN dim_date d ON a.ingestion_date_key = d.date_key