WITH src AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'customers') }}
),
-- Deduplicate on natural_key, keeping the earliest record by ingestion_ts
deduplicated AS (
  SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                  PARTITION BY natural_key
                  ORDER BY customer_id
                  ) AS _row_num
        FROM src
        )
    WHERE _row_num = 1
),

typed AS (
  SELECT
    CAST(customer_id AS bigint) AS customer_id,
    natural_key,
    {{ trim_string('first_name') }} AS first_name,
    {{ trim_string('last_name') }} AS last_name,
    email,
    phone,
    address_line1, address_line2, city, state_region, postcode, country_code,
    CAST(latitude AS double) AS latitude,
    CAST(longitude AS double) AS longitude,
    CAST(birth_date AS date) AS birth_date,
    {{ derive_age('birth_date') }} AS age,
    {{ convert_to_utc('join_ts') }} AS join_ts,
    {{ handle_null('is_vip', 'false') }} AS is_vip,
    {{ handle_null('gdpr_consent', 'false') }} AS gdpr_consent
  FROM deduplicated
)
SELECT * FROM typed