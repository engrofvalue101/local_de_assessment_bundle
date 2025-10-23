-- models/gold/facts/fct_ingestion_audit.sql
{{
    config(
        materialized='incremental',
        unique_key='audit_key',
        on_schema_change='merge',
        schema='gold'
    )
}}

-- Union all staging models to get ingestion metrics
WITH all_ingestions AS (
    SELECT 
        'customers' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count  -- No explicit rejects in current pipeline
    FROM {{ ref('stg_customers') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'orders' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_orders') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'order_lines' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        -- Count invalid lines as rejects
        COUNT(*) FILTER (WHERE qty <= 0 OR unit_price <= 0) AS reject_count
    FROM {{ ref('stg_order_lines') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'products' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_products') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'stores' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_stores') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'suppliers' AS table_name,
        ingestion_ts,
        src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_suppliers') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'events' AS table_name,
        ingestion_ts,
        NULL AS src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'returns' AS table_name,
        ingestion_ts,
        NULL AS src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_returns') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'shipments' AS table_name,
        ingestion_ts,
        NULL AS src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_shipments') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'sensors' AS table_name,
        ingestion_ts,
        NULL AS src_filename,
        COUNT(*) AS row_count,
        -- Count anomalies as rejects
        COUNT(*) FILTER (WHERE temperature_c < -20 OR temperature_c > 50 OR humidity_pct < 10 OR humidity_pct > 95) AS reject_count
    FROM {{ ref('stg_sensors') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
    
    UNION ALL
    
    SELECT 
        'exchange_rates' AS table_name,
        ingestion_ts,
        NULL AS src_filename,
        COUNT(*) AS row_count,
        0 AS reject_count
    FROM {{ ref('stg_exchange_rates') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
    GROUP BY table_name, ingestion_ts, src_filename
),

dim_date AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

ingestion_metrics AS (
    SELECT
        table_name,
        ingestion_ts,
        CAST(ingestion_ts AS DATE) AS ingestion_date,
        src_filename,
        row_count,
        reject_count,
        row_count - reject_count AS rows_accepted,
        
        -- Calculate reject rate
        CASE 
            WHEN row_count > 0 
            THEN (reject_count::FLOAT / row_count::FLOAT) * 100
            ELSE 0 
        END AS reject_rate_pct,
        
        -- Estimate file size (rough estimate: 100 bytes per row)
        -- In a real scenario, this would come from actual file metadata
        row_count * 100 AS file_size_bytes,
        
        -- Estimate processing time based on row count
        -- In a real scenario, this would be tracked by your ingestion pipeline
        CASE
            WHEN row_count < 1000 THEN 1
            WHEN row_count < 10000 THEN 5
            WHEN row_count < 100000 THEN 30
            ELSE 60
        END AS processing_time_seconds,
        
        -- Extract hour for time-based analysis
        EXTRACT(HOUR FROM ingestion_ts) AS ingestion_hour
        
    FROM all_ingestions
),

audit_enriched AS (
    SELECT
        m.*,
        
        -- Calculated metrics
        m.file_size_bytes::FLOAT / 1024 / 1024 AS file_size_mb,
        
        CASE
            WHEN m.processing_time_seconds > 0
            THEN m.row_count::FLOAT / m.processing_time_seconds::FLOAT
            ELSE 0
        END AS rows_per_second,
        
        CASE
            WHEN m.processing_time_seconds > 0
            THEN (m.file_size_bytes::FLOAT / m.processing_time_seconds::FLOAT) / 1024 / 1024
            ELSE 0
        END AS mb_per_second,
        
        -- Data quality tier
        CASE
            WHEN m.reject_rate_pct = 0 THEN 'Excellent'
            WHEN m.reject_rate_pct < 1 THEN 'Good'
            WHEN m.reject_rate_pct < 5 THEN 'Fair'
            ELSE 'Poor'
        END AS data_quality_tier,
        
        -- Processing status
        CASE
            WHEN m.row_count > 0 THEN 'SUCCESS'
            ELSE 'EMPTY'
        END AS processing_status,
        
        -- Performance tier
        CASE 
            WHEN (m.row_count::FLOAT / NULLIF(m.processing_time_seconds, 0)::FLOAT) > 10000 THEN 'Fast'
            WHEN (m.row_count::FLOAT / NULLIF(m.processing_time_seconds, 0)::FLOAT) > 1000 THEN 'Normal'
            WHEN m.processing_time_seconds > 0 THEN 'Slow'
            ELSE 'Unknown'
        END AS processing_speed_tier,
        
        -- Status flags
        CASE WHEN m.row_count > 0 THEN TRUE ELSE FALSE END AS is_success,
        CASE WHEN m.reject_count > 0 THEN TRUE ELSE FALSE END AS has_rejects,
        CASE WHEN m.row_count = 0 THEN TRUE ELSE FALSE END AS is_empty
        
    FROM ingestion_metrics m
),

audit_with_keys AS (
    SELECT
        -- Surrogate key
        {{ dbt_utils.generate_surrogate_key(['a.table_name', 'a.ingestion_ts', "COALESCE(a.src_filename, 'no_file')"] ) }} AS audit_key,
        
        -- Foreign key
        d.date_key AS ingestion_date_key,
        
        -- Dimensions
        a.table_name,
        a.src_filename,
        a.ingestion_ts,
        a.ingestion_date,
        a.ingestion_hour,
        
        -- Volume measures
        a.row_count AS rows_processed,
        a.reject_count AS rows_rejected,
        a.rows_accepted,
        a.file_size_bytes,
        a.file_size_mb,
        
        -- Performance measures
        a.processing_time_seconds,
        a.rows_per_second,
        a.mb_per_second,
        
        -- Quality measures
        a.reject_rate_pct,
        a.data_quality_tier,
        
        -- Status attributes
        a.processing_status,
        a.processing_speed_tier,
        
        -- Status flags
        a.is_success,
        a.has_rejects,
        a.is_empty,
        
        -- Audit
        CURRENT_TIMESTAMP AS fact_created_at
        
    FROM audit_enriched a
    INNER JOIN dim_date d ON a.ingestion_date = d.date_day
)

SELECT * FROM audit_with_keys
{% if is_incremental() %}
    WHERE audit_key NOT IN (SELECT audit_key FROM {{ this }})
{% endif %}