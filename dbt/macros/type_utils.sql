{% macro safe_cast(column, data_type) %}
    TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}

{% macro convert_to_utc(column_name) %}
    timezone('UTC', {{ column_name }})
{% endmacro %}