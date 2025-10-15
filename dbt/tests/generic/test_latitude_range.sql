-- ============================================================================
-- TEST: Latitude Range (-90 to 90)
-- File: tests/generic/test_latitude_range.sql
-- ============================================================================
{% test latitude_range(model, column_name) %}

select
    {{ column_name }} as invalid_latitude,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < -90 or {{ column_name }} > 90)
group by {{ column_name }}

{% endtest %}