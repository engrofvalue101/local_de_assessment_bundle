-- ============================================================================
-- TEST: Discount Percentage Range (0-1 as decimal)
-- ============================================================================
{% test discount_percentage_range(model, column_name) %}

select
    {{ column_name }} as invalid_discount,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and ({{ column_name }} < 0 or {{ column_name }} > 1) 
group by {{ column_name }}

{% endtest %}