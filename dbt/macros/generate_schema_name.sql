{#
    Override de la macro DBT par défaut.
    Comportement standard : <target.schema>_<custom_schema>
    Comportement ici     : <custom_schema> (sans préfixe)
    → réutilise les schémas Snowflake STAGING et MARTS créés au bootstrap.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if custom_schema_name is none -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
