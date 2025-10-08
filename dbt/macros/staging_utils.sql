-- macros/staging_utils.sql
{% macro trim_string(column_name) %}
    nullif(trim({{ column_name }}), '')
{% endmacro %}

{% macro safe_cast(column, data_type) %}
    TRY_CAST({{ column }} AS {{ data_type }})
{% endmacro %}
git push 
{% macro convert_to_utc(column_name) %}
    timezone('UTC', {{ column_name }})
{% endmacro %}

{% macro handle_null(column_name, default_value='unknown') %}
    coalesce({{ column_name }}, '{{ default_value }}')
{% endmacro %}

{% macro derive_age(birth_date_col) %}
    date_part('year', age({{ birth_date_col }}))
{% endmacro %}

{% macro parse_json(json_col, field) %}
    {{ json_col }}->>'{{ field }}'
{% endmacro %}

{% macro calculate_order_total(price_col, qty_col) %}
    {{ price_col }} * {{ qty_col }}
{% endmacro %}