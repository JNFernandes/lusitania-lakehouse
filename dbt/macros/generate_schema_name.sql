{#
  By default dbt names a custom schema "<default>_<custom>" (e.g. main_gold).
  This override uses the custom name as it is, so tables land in
  "staging" and "gold" — clearer when you browse the database.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
