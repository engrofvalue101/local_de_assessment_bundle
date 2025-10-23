-- models/gold/dimensions/dim_customers.sql
{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH customer_snapshot AS (
    SELECT * FROM {{ ref('customers_snapshot') }}
     WHERE is_valid_record = TRUE
),

customer_order_metrics AS (
    SELECT
        customer_id,
        COUNT(DISTINCT order_id) AS total_orders,
        SUM(order_gross_amount) AS lifetime_gross_revenue,
        SUM(order_net_amount) AS lifetime_net_revenue,
        SUM(order_total) AS lifetime_total_revenue,
        AVG(order_net_amount) AS avg_order_value,
        MIN(order_ts) AS first_order_date,
        MAX(order_ts) AS last_order_date,
        EXTRACT(DAY FROM (CURRENT_TIMESTAMP - MAX(order_ts))) AS days_since_last_order,
        CASE
            WHEN COUNT(DISTINCT order_id) > 1 
            THEN EXTRACT(DAY FROM (MAX(order_ts) - MIN(order_ts))) / (COUNT(DISTINCT order_id) - 1)
            ELSE NULL
        END AS avg_days_between_orders
    FROM {{ ref('silver_orders') }}
    WHERE is_valid_record = TRUE
    GROUP BY customer_id
),

customers_enhanced AS (
    SELECT
        c.customer_id,
        
        -- SCD2 columns from snapshot
        c.dbt_scd_id,
        c.dbt_valid_from,
        c.dbt_valid_to,
        CASE WHEN c.dbt_valid_to IS NULL THEN TRUE ELSE FALSE END AS is_current,
        
        -- GDPR-compliant PII handling
        CASE 
            WHEN c.gdpr_consent = FALSE THEN 'MASKED'
            ELSE c.first_name
        END AS first_name,
        
        CASE 
            WHEN c.gdpr_consent = FALSE THEN 'MASKED'
            ELSE c.last_name
        END AS last_name,
        
        CASE 
            WHEN c.gdpr_consent = FALSE 
            THEN MD5(c.email) || '@masked.local'
            ELSE c.email
        END AS email,
        
        CASE 
            WHEN c.gdpr_consent = FALSE THEN 'XXX-XXX-' || RIGHT(c.phone, 4)
            ELSE c.phone
        END AS phone,
        
        -- Address - generalize to city level when consent is false
        CASE 
            WHEN c.gdpr_consent = FALSE THEN NULL
            ELSE c.address_line1
        END AS address_line1,
        
        CASE 
            WHEN c.gdpr_consent = FALSE THEN NULL
            ELSE c.address_line2
        END AS address_line2,
        
        c.city,
        c.state_region,
        
        CASE 
            WHEN c.gdpr_consent = FALSE THEN NULL
            ELSE c.postcode
        END AS postcode,
        
        c.country_code,
        c.latitude,
        c.longitude,
        
        -- Demographic attributes (calculate from snapshot data)
        c.birth_date,
        
        -- Calculate age at the time of this snapshot version
        c.customer_age,
        
        -- Age group based on snapshot version date
        c.age_group,
        
        -- Account attributes
        c.join_ts,
        
        -- Calculate lifetime days at time of this snapshot version
        c.customer_lifetime_days,
        
        CASE
            WHEN c.join_ts IS NOT NULL AND EXTRACT(DAY FROM (c.dbt_valid_from - c.join_ts)) < 90 THEN 'New'
            WHEN c.join_ts IS NOT NULL AND EXTRACT(DAY FROM (c.dbt_valid_from - c.join_ts)) < 365 THEN 'Active'
            WHEN c.join_ts IS NOT NULL AND EXTRACT(DAY FROM (c.dbt_valid_from - c.join_ts)) < 730 THEN 'Established'
            WHEN c.join_ts IS NOT NULL THEN 'Veteran'
            ELSE 'Unknown'
        END AS customer_tenure_segment,
        
        c.is_vip,
        
        -- Customer segment based on snapshot version
        c.customer_segment,        
        c.gdpr_consent,
        
        -- Behavioral metrics (from aggregated orders - these are cumulative, not time-bound)
        COALESCE(m.total_orders, 0) AS total_orders,
        COALESCE(m.lifetime_gross_revenue, 0) AS lifetime_gross_revenue,
        COALESCE(m.lifetime_net_revenue, 0) AS lifetime_net_revenue,
        COALESCE(m.avg_order_value, 0) AS avg_order_value,
        COALESCE(m.days_since_last_order, 9999) AS days_since_last_order,
        m.first_order_date,
        m.last_order_date,
        
        -- Enhanced customer segmentation
        CASE
            WHEN c.is_vip = TRUE THEN 'VIP'
            WHEN COALESCE(m.lifetime_net_revenue, 0) >= 10000 THEN 'High Value'
            WHEN COALESCE(m.lifetime_net_revenue, 0) >= 5000 THEN 'Medium Value'
            WHEN COALESCE(m.total_orders, 0) >= 5 THEN 'Repeat'
            WHEN COALESCE(m.total_orders, 0) >= 1 THEN 'One-time'
            ELSE 'Prospect'
        END AS value_segment,
        
        -- RFM indicators
        CASE
            WHEN COALESCE(m.days_since_last_order, 9999) <= 30 THEN 'Active'
            WHEN COALESCE(m.days_since_last_order, 9999) <= 90 THEN 'Recent'
            WHEN COALESCE(m.days_since_last_order, 9999) <= 180 THEN 'Lapsing'
            WHEN COALESCE(m.days_since_last_order, 9999) <= 365 THEN 'At Risk'
            ELSE 'Inactive'
        END AS recency_status,
        
        -- Audit columns
        c.ingestion_ts,
        CURRENT_TIMESTAMP AS gold_updated_at
        
    FROM customer_snapshot c
    LEFT JOIN customer_order_metrics m 
        ON c.customer_id = m.customer_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['dbt_scd_id']) }} AS customer_key,
    *
FROM customers_enhanced
WHERE customer_id IS NOT NULL