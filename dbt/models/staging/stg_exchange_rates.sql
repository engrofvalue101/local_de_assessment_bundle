WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'exchange_rates') }}
),
cleaned AS (
  SELECT
    {{ safe_cast('date', 'date') }} AS date,
    {{ trim_string('currency') }} AS currency,
    {{ safe_cast('rate_to_aud', 'decimal(18,8)') }} AS rate_to_aud,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned