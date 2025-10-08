{% macro source_reader(source_name, table_name) %}
  {# 
    Macro : source_reader
    Author: Lorenz Alay-ay
    
    Purpose: 
    Dynamically reads data from file-based sources (Parquet or Delta) by looking up 
    metadata from dbt _sources.yml configuration.
    
    Parameters:
      - source_name (string): The name of the source defined in sources.yml
      - table_name (string): The name of the table within that source
    
    Returns:
      A DuckDB table function call (read_parquet or delta_scan) with the file path
    
    Example Usage:
      SELECT * FROM {{ source_reader('bronze_parquet', 'customers') }}
    
    Requirements:
      - Source must be defined in _sources.yml
      - Source must have meta.file_path defined
      - Source must have meta.file_format defined (defaults to 'parquet')
  #}
  
  {# Only execute during actual compilation, not during parsing phase #}
  {%- if execute -%}
    
    {# Convert graph.sources.values() to a list for proper Jinja filtering #}
    {%- set all_sources = graph.sources.values() | list -%}
    
    {# 
      Find the matching source node by filtering on:
      - source_name: matches the source name in sources.yml
      - name: matches the table name in sources.yml
      Returns the first (and should be only) match
    #}
    {%- set source_node = all_sources | selectattr('source_name', 'equalto', source_name) | selectattr('name', 'equalto', table_name) | first -%}
    
    {# Error handling: raise error if source is not found #}
    {%- if not source_node -%}
      {{ exceptions.raise_compiler_error("Source '" ~ source_name ~ "." ~ table_name ~ "' not found in sources.yml") }}
    {%- endif -%}
    
    {# Extract metadata from the source node, default to empty dict if not present #}
    {%- set meta = source_node.meta | default({}) -%}
    
    {# Get file format from metadata, default to 'parquet' if not specified #}
    {%- set file_format = meta.get('file_format', 'parquet') -%}
    
    {# Get file path from metadata - this is required #}
    {%- set file_path = meta.get('file_path') -%}
    
    {# Error handling: file_path is mandatory in metadata #}
    {%- if not file_path -%}
      {{ exceptions.raise_compiler_error("file_path not found in meta for source '" ~ source_name ~ "." ~ table_name ~ "'") }}
    {%- endif -%}
    
    {# 
      Generate the appropriate DuckDB table function based on file format:
      - parquet: uses read_parquet() function
      - delta: uses delta_scan() function
      - other: raises error for unsupported formats
    #}
    {%- if file_format == 'parquet' -%}
read_parquet('{{ file_path }}')
    {%- elif file_format == 'delta' -%}
delta_scan('{{ file_path }}')
    {%- else -%}
      {{ exceptions.raise_compiler_error("Unsupported file format: " ~ file_format) }}
    {%- endif -%}
    
  {%- else -%}
    {# During parsing phase, return empty string to avoid errors #}
{{ return('') }}
  {%- endif -%}
{% endmacro %}