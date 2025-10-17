-- models/staging/stg_exchange_rates.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'exchange_rates') }}
),

renamed AS (
  SELECT
    -- Date and currency
    {{ safe_cast('date', 'date') }} AS rate_date,
    {{ trim_string('currency') }} AS currency,

    -- Rate
    {{ safe_cast('rate_to_aud', 'decimal(18,8)') }} AS rate_to_aud,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash
  FROM source
)

SELECT * FROM renamed