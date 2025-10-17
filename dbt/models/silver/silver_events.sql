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
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

-- Enrichment
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
        
        -- Derived: Time components
        EXTRACT(HOUR FROM event_ts) AS event_hour,
        EXTRACT(DOW FROM event_ts) AS event_day_of_week,
        
        -- Derived: Is weekend
        CASE 
            WHEN EXTRACT(DOW FROM event_ts) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        -- Derived: Event category
        CASE
            WHEN event_type IN ('page_view', 'click') THEN 'Browse'
            WHEN event_type IN ('add_to_cart', 'remove_from_cart') THEN 'Cart'
            WHEN event_type IN ('checkout', 'purchase') THEN 'Conversion'
            WHEN event_type = 'search' THEN 'Search'
            ELSE 'Other'
        END AS event_category,
        
        -- Derived: User type (check if user_id exists in customers)
        CASE 
            WHEN user_id IS NOT NULL THEN 'Registered' 
            ELSE 'Anonymous' 
        END AS user_type,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM events_raw
)

SELECT * FROM events_enriched