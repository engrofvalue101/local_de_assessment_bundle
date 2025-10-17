{% snapshot customers_snapshot %}

{{
    config(
      target_schema='snapshots',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='ingestion_ts::TIMESTAMP',
      invalidate_hard_deletes=True
    )
}}

select
    customer_id,
    natural_key,
    first_name,
    last_name,
    email,
    phone,
    address_line1,
    address_line2,
    city,
    state_region,
    postcode,
    country_code,
    latitude,
    longitude,
    birth_date,
    join_ts,
    is_vip,
    gdpr_consent,
    ingestion_ts
from {{ ref('silver_customers') }}

{% endsnapshot %}