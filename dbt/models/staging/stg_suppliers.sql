WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'suppliers') }}
),
cleaned AS (
  SELECT
    {{ safe_cast('supplier_id', 'bigint') }} AS supplier_id,
    {{ trim_string('supplier_code') }} AS supplier_code,
    {{ trim_string('name') }} AS supplier_name,
    {{ trim_string('country_code') }} AS country_code,
    {{ safe_cast('lead_time_days', 'integer') }} AS lead_time_days,
    {{ safe_cast(handle_null('preferred', 'false'), 'boolean') }} AS preferred,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned