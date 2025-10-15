-- ============================================================================
-- TEST: Email Format Validation
-- File: tests/generic/test_email_format.sql
-- ============================================================================
{% test email_format(model, column_name) %}

select
    {{ column_name }} as invalid_email,
    count(*) as violation_count
from {{ model }}
where {{ column_name }} is not null
  and (
    -- Check for basic email pattern: has @ symbol and domain
    {{ column_name }} not like '%@%.%'
    -- Check for invalid characters
    or {{ column_name }} like '%[%' 
    or {{ column_name }} like '%]%'
    or {{ column_name }} like '%(%'
    or {{ column_name }} like '%)%'
    or {{ column_name }} like '% %'
    -- Check for multiple @ symbols
    or length({{ column_name }}) - length(replace({{ column_name }}, '@', '')) > 1
    -- Check for @ at start or end
    or {{ column_name }} like '@%'
    or {{ column_name }} like '%@'
    -- Check for dot at start or end
    or {{ column_name }} like '.%'
    or {{ column_name }} like '%.'
    -- Check for consecutive dots
    or {{ column_name }} like '%..%'
  )
group by {{ column_name }}

{% endtest %}