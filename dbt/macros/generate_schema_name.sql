{# Utilise les noms de schémas littéraux (bronze / silver / gold / snapshots)
   au lieu du préfixage dbt par défaut <target>_<custom>. #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
