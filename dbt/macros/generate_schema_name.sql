{# A macro generate_schema_name padrão do dbt prefixa um schema customizado
  com o schema padrão do target (ex.: "main_silver"), o que não bate com o
  namespace do S3 Tables que o Terraform realmente cria ("silver" — ver
  infra/s3tables.tf). Usa o schema customizado como está, sem prefixo.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
