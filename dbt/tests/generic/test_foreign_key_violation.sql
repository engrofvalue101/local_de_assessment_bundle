-- ============================================================================
-- TEST: Foreign Key Violation Rate (< 1.5%)
-- File: tests/generic/test_foreign_key_violation_rate.sql
-- ============================================================================
{% test foreign_key_violation_rate(model, column_name, to, field, threshold=1.5) %}

with source_data as (
    select
        {{ column_name }} as fk_value
    from {{ model }}
    where {{ column_name }} is not null
),

target_data as (
    select
        {{ field }} as pk_value
    from {{ to }}
),

violations as (
    select
        s.fk_value
    from source_data s
    left join target_data t
        on s.fk_value = t.pk_value
    where t.pk_value is null
),

metrics as (
    select
        count(distinct s.fk_value) as total_fk_count,
        count(distinct v.fk_value) as violation_count,
        case
            when count(distinct s.fk_value) = 0 then 0
            else (count(distinct v.fk_value) * 100.0) / count(distinct s.fk_value)
        end as violation_rate
    from source_data s
    left join violations v
        on s.fk_value = v.fk_value
)

select
    total_fk_count,
    violation_count,
    violation_rate,
    {{ threshold }} as threshold
from metrics
where violation_rate >= {{ threshold }}

{% endtest %}