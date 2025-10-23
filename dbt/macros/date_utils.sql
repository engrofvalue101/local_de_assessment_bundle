{% macro calculate_age_years(birth_date_col) -%}
    extract(year from age(current_date, {{ birth_date_col }}))
{%- endmacro %}

{% macro calculate_customer_lifetime_days(join_ts_col) -%}
    case
        when {{ join_ts_col }} is not null
        then date_diff('day', {{ join_ts_col }}, current_timestamp)
        else null
    end
{%- endmacro %}