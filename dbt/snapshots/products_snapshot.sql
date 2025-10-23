{% snapshot products_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=[
        'current_price',
        'is_discontinued',
        'discontinued_dt',
        'is_valid_record'
      ],
      invalidate_hard_deletes=True
    )
}}

-- Include validation flags to track data quality changes over time
select
    product_id,
    sku,
    product_name,
    category,
    subcategory,
    current_price,
    currency,
    is_discontinued,
    introduced_dt,
    discontinued_dt,
    
    -- Quality validation flags (track quality changes over time)
    has_null_product_id,
    has_null_sku,
    has_negative_price,
    has_discontinued_before_introduced,
    has_discontinued_without_date,
    is_valid_record,
    quality_issue_type,
    
    -- Audit
    ingestion_ts,
    transformed_at
    
from {{ ref('silver_products') }}
-- where is_valid_record = TRUE

{% endsnapshot %}