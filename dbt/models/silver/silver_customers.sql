-- models/silver/silver_customers.sql
{{
    config(
        materialized='table',
        schema='silver'
    )
}}

WITH customers_raw AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

-- Deduplication by natural_key
customers_deduped AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY natural_key 
            ORDER BY 
                ingestion_ts DESC,
                is_vip DESC,
                gdpr_consent DESC,
                customer_id DESC
        ) AS row_num
    FROM customers_raw
),

-- Enrichment
customers_enriched AS (
    SELECT
        customer_id,
        natural_key,
        first_name,
        last_name,
        email,
        phone,
        address_line1,
        address_line2,
        city,
        state_region,
        postcode,
        country_code,
        latitude,
        longitude,
        birth_date,
        join_ts,
        is_vip,
        gdpr_consent,
        
        -- Derived: Customer age
        {{ calculate_age_years('birth_date') }} AS customer_age,
        
        -- Derived: Customer tenure (DuckDB inline)
         {{ calculate_customer_lifetime_days('join_ts') }} AS customer_lifetime_days,
        
        -- Derived: Age group (DuckDB inline)
        {{ categorize_age_group('birth_date') }} AS age_group,
        
        -- Derived: Customer segment
        {{ categorize_customer_segment('is_vip', 'join_ts') }} AS customer_segment,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM customers_deduped
    WHERE row_num = 1
)

SELECT * FROM customers_enriched