{% test staging_freshness(model, column_name, warn_after_hours=12, error_after_hours=24) %}

{#
    This test checks if the data in a staging model is fresh enough.
    It mimics dbt's source freshness behavior but works on models.
    
    Usage in _staging.yml:
    
    models:
      - name: stg_customers
        tests:
          - staging_freshness:
              column_name: ingestion_ts
              warn_after_hours: 12
              error_after_hours: 24
              config:
                severity: warn  # or error
#}

with latest_data as (
    select 
        max({{ column_name }}) as max_loaded_at,
        current_timestamp as current_ts
    from {{ model }}
),

freshness_check as (
    select
        max_loaded_at,
        current_ts,
        extract(epoch from (current_ts - max_loaded_at)) / 3600 as hours_since_load,
        {{ warn_after_hours }} as warn_threshold_hours,
        {{ error_after_hours }} as error_threshold_hours,
        case
            when max_loaded_at is null then 'NO_DATA'
            when extract(epoch from (current_ts - max_loaded_at)) / 3600 > {{ error_after_hours }}
                then 'ERROR'
            when extract(epoch from (current_ts - max_loaded_at)) / 3600 > {{ warn_after_hours }}
                then 'WARN'
            else 'PASS'
        end as freshness_status
    from latest_data
)

select
    max_loaded_at,
    current_ts,
    hours_since_load,
    freshness_status,
    'Data is stale! Last loaded ' || round(hours_since_load::numeric, 2) || ' hours ago' as message
from freshness_check
where freshness_status in ('ERROR', 'NO_DATA')

{% endtest %}