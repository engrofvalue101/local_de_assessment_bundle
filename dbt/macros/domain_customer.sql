{% macro categorize_age_group(birth_date_col) -%}
    case
        when {{ birth_date_col }} is null then 'Unknown'
        when extract(year from age(current_date, {{ birth_date_col }})) < 18 then 'Under 18'
        when extract(year from age(current_date, {{ birth_date_col }})) between 18 and 24 then '18-24'
        when extract(year from age(current_date, {{ birth_date_col }})) between 25 and 34 then '25-34'
        when extract(year from age(current_date, {{ birth_date_col }})) between 35 and 44 then '35-44'
        when extract(year from age(current_date, {{ birth_date_col }})) between 45 and 54 then '45-54'
        when extract(year from age(current_date, {{ birth_date_col }})) between 55 and 64 then '55-64'
        when extract(year from age(current_date, {{ birth_date_col }})) >= 65 then '65+'
        else 'Unknown'
    end
{%- endmacro %}

{% macro categorize_customer_segment(is_vip_col, join_ts_col) -%}
    case
        when {{ is_vip_col }} = true and date_diff('day', {{ join_ts_col }}, current_timestamp) > 365 then 'VIP - Long Term'
        when {{ is_vip_col }} = true and date_diff('day', {{ join_ts_col }}, current_timestamp) <= 365 then 'VIP - New'
        when {{ is_vip_col }} = false and date_diff('day', {{ join_ts_col }}, current_timestamp) > 365 then 'Regular - Long Term'
        when {{ is_vip_col }} = false and date_diff('day', {{ join_ts_col }}, current_timestamp) <= 365 then 'Regular - New'
        else 'Unknown'
    end
{%- endmacro %}