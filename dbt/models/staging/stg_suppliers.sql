-- models/staging/stg_suppliers.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'suppliers') }}
),

renamed AS (
  SELECT
    -- IDs
    {{ safe_cast('supplier_id', 'bigint') }} AS supplier_id,
    {{ trim_string('supplier_code') }} AS supplier_code,

    -- Supplier info
    {{ trim_string('name') }} AS supplier_name,
    {{ trim_string('country_code') }} AS country_code,
    {{ safe_cast('lead_time_days', 'integer') }} AS lead_time_days,
    {{ safe_cast(handle_null('preferred', 'false'), 'boolean') }} AS preferred,

    -- Audit columns
    {{ convert_to_utc('ingestion_ts') }} AS ingestion_ts,
    src_filename,
    src_row_hash

  FROM source
)

SELECT * FROM renamed 