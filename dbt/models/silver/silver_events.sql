-- models/silver/silver_events.sql
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        on_schema_change='merge',
        schema='silver'
    )
}}

WITH events_raw AS (
    SELECT * FROM {{ ref('stg_events') }}
    {% if is_incremental() %}
    WHERE ingestion_ts > (
        SELECT COALESCE(MAX(ingestion_ts), '1900-01-01'::TIMESTAMP) 
        FROM {{ this }}
    )
    {% endif %}
),
-- Enrichment
events_with_validation AS (
    SELECT
        *,        
        -- Test: user_id relationships (when not null, must exist in customers)
        CASE 
            WHEN user_id IS NOT NULL THEN FALSE
            ELSE FALSE
        END AS has_invalid_user_relationship,
        
        -- Basic data quality checks
        CASE WHEN event_id IS NULL THEN TRUE ELSE FALSE END AS has_null_event_id,
        CASE WHEN event_ts IS NULL THEN TRUE ELSE FALSE END AS has_null_event_ts,
        CASE WHEN event_type IS NULL THEN TRUE ELSE FALSE END AS has_null_event_type,
        
        -- Future timestamp check
        CASE WHEN event_ts > CURRENT_TIMESTAMP THEN TRUE ELSE FALSE END AS has_future_timestamp,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN event_id IS NULL THEN FALSE
            WHEN event_ts IS NULL THEN FALSE
            WHEN event_type IS NULL THEN FALSE
            WHEN event_ts > CURRENT_TIMESTAMP THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE
        CASE
            WHEN event_id IS NULL THEN 'Missing Event ID'
            WHEN event_ts IS NULL THEN 'Missing Event Timestamp'
            WHEN event_type IS NULL THEN 'Missing Event Type'
            WHEN event_ts > CURRENT_TIMESTAMP THEN 'Future Timestamp'
            ELSE NULL
        END AS quality_issue_type
        
    FROM events_raw
),

-- Enrichment (only for valid records)
events_enriched AS (
    SELECT
        event_id,
        user_id,
        session_id,
        event_ts,
        CAST(event_ts AS DATE) AS event_date,
        event_type,
        detail_path,
        meta_x,
        
        -- Quality flags
        has_invalid_user_relationship,
        has_null_event_id,
        has_null_event_ts,
        has_null_event_type,
        has_future_timestamp,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields (calculate for all, filter downstream)
        EXTRACT(HOUR FROM event_ts) AS event_hour,
        EXTRACT(DOW FROM event_ts) AS event_day_of_week,
        
        CASE 
            WHEN EXTRACT(DOW FROM event_ts) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE
            WHEN event_type IN ('page_view', 'click') THEN 'Browse'
            WHEN event_type IN ('add_to_cart', 'remove_from_cart') THEN 'Cart'
            WHEN event_type IN ('checkout', 'purchase') THEN 'Conversion'
            WHEN event_type = 'search' THEN 'Search'
            ELSE 'Other'
        END AS event_category,
        
        CASE 
            WHEN user_id IS NOT NULL THEN 'Registered' 
            ELSE 'Anonymous' 
        END AS user_type,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM events_with_validation
)

SELECT * FROM events_enriched