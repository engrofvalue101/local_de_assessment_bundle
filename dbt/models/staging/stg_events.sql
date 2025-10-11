WITH source AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'events') }}
),
cleaned AS (
  SELECT
    {{ trim_string('event_id') }} AS event_id,
    {{ convert_to_utc(safe_cast('event_ts', 'timestamp')) }} AS event_ts,
    {{ trim_string('event_type') }} AS event_type,
    {{ safe_cast('user_id', 'bigint') }} AS user_id,
    {{ trim_string('session_id') }} AS session_id,
    {{ parse_json_payload('payload_json', 'details.path', 'string') }} AS detail_path,
    {{ parse_json_payload('payload_json', 'details.meta.x', 'int') }} AS meta_x,
    ingestion_ts
  FROM source
)
SELECT * FROM cleaned