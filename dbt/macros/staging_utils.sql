-- macros/staging_utils.sql

{% macro normalize_column_name(column_name) %}
    lower(regexp_replace({{ column_name }}, '[^a-zA-Z0-9]', '_'))
{% endmacro %}

{% macro trim_string(column_name) %}
    nullif(trim({{ column_name }}), '')
{% endmacro %}

{% macro convert_to_utc(column_name) %}
    -- Works in DuckDB; adjust for other warehouses
    timezone('UTC', {{ column_name }})
{% endmacro %}

{% macro handle_null(column_name, default_value='unknown') %}
    coalesce({{ column_name }}, '{{ default_value }}')
{% endmacro %}

{% macro derive_age(birth_date_col) %}
    datediff('year', {{ birth_date_col }}, current_date)
{% endmacro %}

{% macro parse_json(json_col, field) %}
    {{ json_col }}->>'{{ field }}'
{% endmacro %}

{% macro calculate_order_total(price_col, qty_col) %}
    {{ price_col }} * {{ qty_col }}
{% endmacro %}

{% macro deduplicate(table_ref, unique_key) %}
    -- General-purpose deduplication
    select *
    from (
        select *,
               row_number() over (partition by {{ unique_key }} order by updated_at desc) as _row_num
        from {{ table_ref }}
    )
    where _row_num = 1
{% endmacro %}