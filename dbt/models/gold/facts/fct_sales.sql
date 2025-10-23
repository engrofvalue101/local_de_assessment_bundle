-- models/gold/facts/fct_sales.sql
{{
    config(
        materialized='incremental',
        unique_key='sale_key',
        on_schema_change='merge',
        schema='gold'
    )
}}

WITH silver_order_lines AS (
    SELECT 
        order_id,
        line_number,
        product_id,
        customer_id,
        store_id,
        order_ts,
        order_dt_local,
        CAST(order_ts AS DATE) AS order_date,
        EXTRACT(YEAR FROM order_ts) AS order_year,
        EXTRACT(MONTH FROM order_ts) AS order_month,
        EXTRACT(QUARTER FROM order_ts) AS order_quarter,
        channel,
        currency,
        category,
        subcategory,
        qty,
        unit_price,
        line_discount_pct,
        tax_pct,
        gross_amount,
        discount_amount,
        net_amount,
        tax_amount,
        line_total,
        ingestion_ts
    FROM {{ ref('silver_order_lines') }}
    {% if is_incremental() %}
        WHERE ingestion_ts > (SELECT MAX(ingestion_ts) FROM {{ this }})
    {% endif %}
),

dim_customer AS (
    SELECT customer_key, customer_id FROM {{ ref('dim_customers') }}
),

dim_product AS (
    SELECT 
        product_key, 
        product_id,
        is_current
    FROM {{ ref('dim_product_scd') }}
    WHERE is_current = TRUE  -- Join to current product version
),

dim_store AS (
    SELECT store_key, store_id FROM {{ ref('dim_stores') }}
),

dim_date AS (
    SELECT date_key, date_day FROM {{ ref('dim_date') }}
),

sales_with_keys AS (
    SELECT
        -- Surrogate key for this fact
        {{ dbt_utils.generate_surrogate_key(['o.order_id', 'o.line_number']) }} AS sale_key,
        
        -- Foreign keys to dimensions
        c.customer_key,
        p.product_key,
        s.store_key,
        d.date_key,
        
        -- Degenerate dimensions
        o.order_id,
        o.line_number,
        o.channel,
        
        -- Date/time
        o.order_ts,
        o.order_dt_local,
        o.order_year,
        o.order_month,
        o.order_quarter,
        
        -- Measures - Quantities
        o.qty AS quantity,
        
        -- Measures - Amounts
        o.unit_price,
        o.gross_amount,
        o.line_discount_pct AS discount_pct,
        o.discount_amount,
        o.net_amount,
        o.tax_amount,
        o.line_total AS total_amount,
        
        -- Derived metrics
        CASE 
            WHEN o.line_discount_pct > 0 THEN TRUE 
            ELSE FALSE 
        END AS has_discount,
        
        CASE
            WHEN o.line_discount_pct >= 0.20 THEN 'High'
            WHEN o.line_discount_pct >= 0.10 THEN 'Medium'
            WHEN o.line_discount_pct > 0 THEN 'Low'
            ELSE 'None'
        END AS discount_tier,
        
        -- Product category for analysis
        o.category,
        o.subcategory,
        
        -- Audit
        o.ingestion_ts,
        CURRENT_TIMESTAMP AS fact_created_at
        
    FROM silver_order_lines o
    INNER JOIN dim_customer c ON o.customer_id = c.customer_id
    INNER JOIN dim_product p ON o.product_id = p.product_id
    INNER JOIN dim_store s ON o.store_id = s.store_id
    INNER JOIN dim_date d ON o.order_date = d.date_day
)

SELECT * FROM sales_with_keys
{% if is_incremental() %}
    WHERE sale_key NOT IN (SELECT sale_key FROM {{ this }})
{% endif %}