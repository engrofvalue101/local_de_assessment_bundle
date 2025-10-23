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

-- Include validation flags to track data quality changes over time
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
    customer_age,
    customer_lifetime_days,
    age_group,
    customer_segment,
    
    -- Quality validation flags (track quality changes over time)
    has_null_customer_id,
    has_null_natural_key,
    has_invalid_email_format,
    has_invalid_latitude_range,
    has_invalid_longitude_range,
    is_valid_record,
    quality_issue_type,
    
    -- Audit
    ingestion_ts,
    transformed_at
    
from {{ ref('silver_customers') }}
-- where is_valid_record = TRUE

{% endsnapshot %}