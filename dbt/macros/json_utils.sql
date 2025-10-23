{% macro parse_json_payload(json_column, field_path, field_type='string') %}
    {%- if field_type == 'timestamp' -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as timestamp)
    {%- elif field_type == 'date' -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as date)
    {%- elif field_type in ['int', 'integer'] -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as integer)
    {%- elif field_type in ['bigint', 'long'] -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as bigint)
    {%- elif field_type in ['double', 'float'] -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as double)
    {%- elif field_type in ['boolean', 'bool'] -%}
        cast(json_extract_string({{ json_column }}, '$.{{ field_path }}') as boolean)
    {%- else -%}
        json_extract_string({{ json_column }}, '$.{{ field_path }}')
    {%- endif -%}
{% endmacro %}