{% macro source_reader(source_name, table_name) %}
{%- if execute -%}
  {%- set all_sources = graph.sources.values() | list -%}
  {%- set source_node = all_sources | selectattr('source_name', 'equalto', source_name) | selectattr('name', 'equalto', table_name) | first -%}
  {%- if not source_node -%}
    {{ exceptions.raise_compiler_error("Source '" ~ source_name ~ "." ~ table_name ~ "' not found") }}
  {%- endif -%}
  {%- set meta = source_node.meta | default({}) -%}
  {%- set file_format = meta.get('file_format', 'parquet') -%}
  {%- set file_path = meta.get('file_path') -%}
  {%- if not file_path -%}
    {{ exceptions.raise_compiler_error("file_path not found in meta") }}
  {%- endif -%}
  {%- if file_format == 'parquet' -%}
read_parquet('{{ file_path }}')
  {%- elif file_format == 'delta' -%}
delta_scan('{{ file_path }}')
  {%- else -%}
    {{ exceptions.raise_compiler_error("Unsupported file format") }}
  {%- endif -%}
{%- else -%}
__dbt_parse_placeholder_table
{%- endif -%}
{% endmacro %}