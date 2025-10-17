-- models/staging/stg_customers.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'customers') }} 
),

renamed AS (
  SELECT
    -- IDs
    {{ safe_cast('customer_id', 'bigint') }} AS customer_id,
    {{ trim_string('natural_key') }} AS natural_key,

    -- Personal Info
    {{ trim_string('first_name') }} AS first_name,
    {{ trim_string('last_name') }} AS last_name,
    {{ trim_string('email') }} AS email,
    {{ format_phone_number (trim_string('phone')) }} AS phone,

    -- Address Info
    {{ trim_string('address_line1') }} AS address_line1,
    {{ trim_string('address_line2') }} AS address_line2,
    {{ trim_string('city') }} AS city,
    {{ trim_string('state_region') }} AS state_region,
    {{ trim_string('postcode') }} AS postcode,
    {{ trim_string('country_code') }} AS country_code,
    {{ safe_cast('latitude', 'double') }} AS latitude,
    {{ safe_cast('longitude', 'double') }} AS longitude,

    -- Dates
    {{ safe_cast('birth_date', 'date') }} AS birth_date,
    {{ convert_to_utc('join_ts') }} AS join_ts,

    -- Flags
    {{ safe_cast(handle_null('is_vip', 'false'), 'boolean') }} AS is_vip,
    {{ safe_cast(handle_null('gdpr_consent', 'false'), 'boolean') }} AS gdpr_consent,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash
    
  FROM source
)

SELECT * FROM renamed