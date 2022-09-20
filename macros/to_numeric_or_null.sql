{#
keeps as is if already is_number() in the source model, else applies CTE to_numeric_or_null

-- {{  col.data_type if col else 'no col' }}
#}


{% macro to_numeric_or_null(column_name, source, source_alias=None) %}
{% set col = adapter.get_columns_in_relation(source) | selectattr("name", "eq", column_name) | list | first %}
{% if not col or col.is_number() %}
  {{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }}
{% else %}{# assuming text but maybe TODO convert ::text up to test ? #}
  "{{ schema }}".to_numeric_or_null({{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }})
{% endif %}
{% endmacro %}