{#
keeps as is if already is date in the source model, else applies CTE to_date_or_null

TODO : test 'timestamp'..., more than 1 format
#}


{% macro to_date_or_null(column_name, source, fmts=["YYYY-MM-DD HH24:MI:SS"], source_alias=None) %}
{% set col = adapter.get_columns_in_relation(source) | selectattr("name", "eq", column_name) | list | first %}
-- {{  col.data_type if col else 'no col' }}
{% if not col or col.data_type == 'date' or col.data_type == 'timestamp' or col.data_type == 'timestamp with time zone' %}
  {{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }}
{% else %}{# assuming text but maybe TODO convert ::text up to test ? #}
  "{{ schema }}".to_date_or_null({{ source_alias if source_alias else source }}.{{ adapter.quote(column_name) }}::text, '{{ fmts[0] }}'::text)
{% endif %}
{% endmacro %}