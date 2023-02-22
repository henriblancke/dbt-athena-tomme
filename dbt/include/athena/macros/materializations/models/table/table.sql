{% materialization table, adapter='athena' -%}
  {%- set identifier = model['alias'] -%}

  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set target_relation = api.Relation.create(identifier=identifier,
                                                schema=schema,
                                                database=database,
                                                type='table') -%}

  {%- set lf_tags = config.get('lf_tags', default=none) -%}

  {{ run_hooks(pre_hooks) }}

  {%- if old_relation is not none -%}
      {{ adapter.drop_relation(old_relation) }}
  {%- endif -%}

  -- build model
  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {% endcall -%}

  -- set table properties
  {{ set_table_classification(target_relation, 'parquet') }}
  {{ adapter.add_lf_tags_to_table(target_relation.schema, target_relation.table, lf_tags) }}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
