WITH src AS (
  SELECT * FROM {{ source_reader('bronze_delta', 'customers') }}
),
-- Deduplicate on natural_key, keeping the earliest record by ingestion_ts
duplicates AS (
  SELECT *
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (
                  PARTITION BY natural_key
                  ORDER BY customer_id
                  ) AS _row_num
        FROM src
        )
    WHERE _row_num > 1
)

SELECT customer_id, natural_key, join_ts, ingestion_ts
FROM duplicates
ORDER BY 1