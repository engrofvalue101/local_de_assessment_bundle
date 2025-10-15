-- ============================================================================
-- TEST: Longitude Range (-180 to 180)
-- File: tests/generic/test_longitude_range.sql
-- ============================================================================
{% test longitude_range(model, column_name) %}

select
    {{ column_name }} as invalid_longitude,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < -180 or {{ column_name }} > 180)
group by {{ column_name }}

{% endtest %}