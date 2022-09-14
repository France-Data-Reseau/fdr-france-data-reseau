{#
keeps as is if already is_boolean() in the source model, else applies CTE to_numeric_or_null from
#}


{% macro to_boolean_or_null(column_name, source, source_alias=None) %}
{% set col = adapter.get_columns_in_relation(source) | selectattr("name", "eq", column_name) | list | first %}
-- {{  col.data_type if col else 'no col' }}
{% if not col or col.data_type == 'boolean' %}
  {{ source }}.{{ adapter.quote(column_name) }}
{% elif col.is_number() %}
  {{ schema }}.to_boolean_or_null({{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }}::numeric)
{% else %}{# assuming text but maybe TODO convert ::text up to test ? #}
  {{ schema }}.to_boolean_or_null({{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }}::text)
{% endif %}
{% endmacro %}