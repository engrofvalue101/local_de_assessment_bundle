-- models/staging/stg_events.sql
{{
    config(
        materialized='view'
    )
}}

WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'events') }}
),

renamed AS (
  SELECT
    -- IDs
    {{ trim_string('event_id') }} AS event_id,
    {{ safe_cast('user_id', 'bigint') }} AS user_id,
    {{ trim_string('session_id') }} AS session_id,

    -- Event info
    {{ convert_to_utc(safe_cast('event_ts', 'timestamp')) }} AS event_ts,
    {{ trim_string('event_type') }} AS event_type,

    -- Payload JSON parsing
    {{ parse_json_payload('payload_json', 'details.path', 'string') }} AS detail_path,
    {{ parse_json_payload('payload_json', 'details.meta.x', 'int') }} AS meta_x,

    -- Audit columns
    ingestion_ts,
    src_filename,
    src_row_hash

  FROM source
)

SELECT * FROM renamed