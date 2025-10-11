WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'customers') }}
),
deduplicated AS (
  SELECT *
    FROM source
    QUALIFY ROW_NUMBER() OVER (
                  PARTITION BY natural_key
                  ORDER BY customer_id
                  ) = 1
),
cleaned AS (
  SELECT
    {{ safe_cast('customer_id', 'bigint') }} AS customer_id,
    natural_key,
    {{ trim_string('first_name') }} AS first_name,
    {{ trim_string('last_name') }} AS last_name,
    {{ trim_string('email') }} AS email,
    {{ format_phone_number (trim_string('phone')) }} AS phone,
    {{ trim_string('address_line1') }} AS address_line1,
    {{ trim_string('address_line2') }} AS address_line2,
    {{ trim_string('city') }} AS city,
    {{ trim_string('state_region') }} AS state_region,
    {{ trim_string('postcode') }} AS postcode,
    {{ trim_string('country_code') }} AS country_code,
    {{ safe_cast('latitude', 'double') }} AS latitude,
    {{ safe_cast('longitude', 'double') }} AS longitude,
    {{ safe_cast('birth_date', 'date') }} AS birth_date,
    {{ convert_to_utc('join_ts') }} AS join_ts,
    {{ safe_cast(handle_null('is_vip', 'false'), 'boolean') }} AS is_vip,
    {{ safe_cast(handle_null('gdpr_consent', 'false'), 'boolean') }} AS gdpr_consent,
    ingestion_ts
  FROM deduplicated
)
SELECT * FROM cleaned