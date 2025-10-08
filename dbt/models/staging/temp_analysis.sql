{# SELECT * FROM {{ source_reader('bronze_delta', 'products') }} #}
{# SELECT * FROM read_parquet('../lake/bronze/parquet/customers/') #}
{# SELECT * FROM delta_scan('../lake/bronze/delta/customers/') #}
{# SELECT * FROM delta_scan('../lake/bronze/delta/products/') #}

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
                  ORDER BY ingestion_ts
                  ) AS _row_num
        FROM src
        )
    WHERE 1=1
    {# AND _row_num = 1 #}
    AND natural_key IN (
'CUST-0KB68RER',
'CUST-3OXESP7J',
'CUST-3PPMBLC0',
'CUST-5KXQHXTD',
'CUST-5U50FH61',
'CUST-6APFXI5N',
'CUST-8W80UXN3',
'CUST-AWVSPYZF',
'CUST-CF33U699',
'CUST-FLVRKTGU',
'CUST-GQLLX70R',
'CUST-I0Q1RIS5',
'CUST-QKV0N4H2',
'CUST-WERX6TP9',
'CUST-YASYINPF',
'CUST-ZSX0GNZH'
)
)


SELECT customer_id, natural_key, ingestion_ts, join_ts, _row_num FROM deduplicated
ORDER BY natural_key, ingestion_ts, customer_id