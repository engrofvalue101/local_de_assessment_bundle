{% macro trim_string(column_name) %}
    nullif(trim({{ column_name }}), '')
{% endmacro %}

{% macro handle_null(column_name, default_value='unknown') %}
    coalesce({{ column_name }}, '{{ default_value }}')
{% endmacro %}

{% macro format_phone_number(phone_col) %}
    CASE 
        WHEN {{ phone_col }} IS NULL OR TRIM({{ phone_col }}) = '' THEN NULL
        ELSE
            (
                -- Step 1: Remove all non-numeric characters
                REGEXP_REPLACE({{ phone_col }}, '[^0-9]', '', 'g')
            )
    END
{% endmacro %}