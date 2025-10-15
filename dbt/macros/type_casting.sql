-- macros/type_casting.sql
-- Safe type casting and conversion utilities

{% macro safe_cast(column, data_type) %}
    try_cast({{ column }} as {{ data_type }})
{% endmacro %}

{% macro handle_null(column_name, default_value='unknown') %}
    coalesce({{ column_name }}, '{{ default_value }}')
{% endmacro %}