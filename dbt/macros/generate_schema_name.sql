{# dbts default generate_schema_name macro prefixes a custom schema with the
  target's default schema (e.g. "main_silver"), which doesn't match the S3
  Tables namespace Terraform actually creates ("silver" — see
  infra/s3tables.tf). Use the custom schema as-is instead.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
