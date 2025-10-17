{% snapshot products_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='product_id',
      strategy='check',
      check_cols=[
        'current_price',
        'is_discontinued',
        'discontinued_dt'
      ],
      invalidate_hard_deletes=True
    )
}}

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
    ingestion_ts
from {{ ref('silver_products') }}

{% endsnapshot %}