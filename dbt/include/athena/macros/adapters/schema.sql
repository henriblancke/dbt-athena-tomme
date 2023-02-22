{% macro athena__create_schema(relation) -%}
  {%- call statement('create_schema') -%}
    create schema if not exists {{ relation.without_identifier() }}
  {%- endcall -%}

  {{ adapter.add_lf_tags_to_database(relation.schema) }}
{% endmacro %}