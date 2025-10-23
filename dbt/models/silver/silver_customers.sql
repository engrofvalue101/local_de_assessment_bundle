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
                customer_id
        ) AS row_num
    FROM customers_raw
),

-- Add quality validation flags based on staging tests
customers_with_quality_flags AS (
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
        
        -- Test: customer_id unique and not_null
        CASE WHEN customer_id IS NULL THEN TRUE ELSE FALSE END AS has_null_customer_id,
        
        -- Test: natural_key unique and not_null
        CASE WHEN natural_key IS NULL THEN TRUE ELSE FALSE END AS has_null_natural_key,
        
        -- Test: email_format
        CASE 
            WHEN email IS NOT NULL 
                AND (
                    email NOT LIKE '%@%.%'
                    OR email LIKE '%[%' 
                    OR email LIKE '%]%'
                    OR email LIKE '%(%'
                    OR email LIKE '%)%'
                    OR email LIKE '% %'
                    OR LENGTH(email) - LENGTH(REPLACE(email, '@', '')) > 1
                    OR email LIKE '@%'
                    OR email LIKE '%@'
                    OR email LIKE '.%'
                    OR email LIKE '%.'
                    OR email LIKE '%..%'
                )
            THEN TRUE
            ELSE FALSE
        END AS has_invalid_email_format,
        
        -- Test: latitude_range (-90 to 90)
        CASE 
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90)
            THEN TRUE
            ELSE FALSE
        END AS has_invalid_latitude_range,
        
        -- Test: longitude_range (-180 to 180)
        CASE 
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180)
            THEN TRUE
            ELSE FALSE
        END AS has_invalid_longitude_range,
        
        -- OVERALL VALIDITY FLAG
        CASE 
            WHEN customer_id IS NULL THEN FALSE
            WHEN natural_key IS NULL THEN FALSE
            WHEN email IS NOT NULL 
                AND (
                    email NOT LIKE '%@%.%'
                    OR email LIKE '%[%' 
                    OR email LIKE '%]%'
                    OR email LIKE '%(%'
                    OR email LIKE '%)%'
                    OR email LIKE '% %'
                    OR LENGTH(email) - LENGTH(REPLACE(email, '@', '')) > 1
                    OR email LIKE '@%'
                    OR email LIKE '%@'
                    OR email LIKE '.%'
                    OR email LIKE '%.'
                    OR email LIKE '%..%'
                )
            THEN FALSE
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90) THEN FALSE
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180) THEN FALSE
            ELSE TRUE
        END AS is_valid_record,
        
        -- QUALITY ISSUE TYPE (for easy filtering/reporting)
        CASE
            WHEN customer_id IS NULL THEN 'Missing Customer ID'
            WHEN natural_key IS NULL THEN 'Missing Natural Key'
            WHEN email IS NOT NULL 
                AND (
                    email NOT LIKE '%@%.%'
                    OR email LIKE '%[%' 
                    OR email LIKE '%]%'
                    OR email LIKE '%(%'
                    OR email LIKE '%)%'
                    OR email LIKE '% %'
                    OR LENGTH(email) - LENGTH(REPLACE(email, '@', '')) > 1
                    OR email LIKE '@%'
                    OR email LIKE '%@'
                    OR email LIKE '.%'
                    OR email LIKE '%.'
                    OR email LIKE '%..%'
                )
            THEN 'Invalid Email Format'
            WHEN latitude IS NOT NULL AND (latitude < -90 OR latitude > 90) THEN 'Invalid Latitude Range'
            WHEN longitude IS NOT NULL AND (longitude < -180 OR longitude > 180) THEN 'Invalid Longitude Range'
            ELSE NULL
        END AS quality_issue_type,
        
        ingestion_ts
        
    FROM customers_deduped
    WHERE row_num = 1
),

-- Enrichment (only calculate derived fields for valid customers)
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
        
        -- Quality flags
        has_null_customer_id,
        has_null_natural_key,
        has_invalid_email_format,
        has_invalid_latitude_range,
        has_invalid_longitude_range,
        is_valid_record,
        quality_issue_type,
        
        -- Derived fields (only for valid records)
        CASE 
            WHEN is_valid_record THEN {{ calculate_age_years('birth_date') }}
            ELSE NULL
        END AS customer_age,
        
        CASE 
            WHEN is_valid_record THEN {{ calculate_customer_lifetime_days('join_ts') }}
            ELSE NULL
        END AS customer_lifetime_days,
        
        CASE 
            WHEN is_valid_record THEN {{ categorize_age_group('birth_date') }}
            ELSE 'Invalid'
        END AS age_group,
        
        CASE 
            WHEN is_valid_record THEN {{ categorize_customer_segment('is_vip', 'join_ts') }}
            ELSE 'Invalid'
        END AS customer_segment,
        
        -- Audit
        ingestion_ts,
        CURRENT_TIMESTAMP AS transformed_at
        
    FROM customers_with_quality_flags
)

SELECT * FROM customers_enriched