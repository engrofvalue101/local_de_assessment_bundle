-- macros/generate_staging_yaml.sql

{% macro get_models_by_path(path_pattern='staging', prefix='stg_') %}
  {% set models = [] %}
  {% for node in graph.nodes.values() %}
    {% if node.resource_type == 'model' 
       and node.name.startswith(prefix)
       and path_pattern in node.path %}
      {% do models.append(node.name) %}
    {% endif %}
  {% endfor %}
  {{ return(models) }}
{% endmacro %}

{% macro generate_yaml_by_prefix(path_pattern='staging', prefix='stg_') %}
  {% set model_list = get_models_by_path(path_pattern, prefix) %}
  {{ log("Generating YAML for " ~ model_list | length ~ " models", info=true) }}
  {{ codegen.generate_model_yaml(model_names=model_list) }}
{% endmacro %}