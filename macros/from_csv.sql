{#
TODO from geojson or polyline (to_csv()), json : according to param, example data, meta ?

lecture de version csv-isable (préparée pour export CSV) :
- de geojson-ize geo fields
TODO convert sr if required
TODO handle custom convert (point without POINT) ? NO that's the point of _translated
TODO so help _translated by cols_from_csv() ?
TODO keep original ? Superset supports geohash, Polyline, geojson. But the only
way Polyline is served on the way is through geoserver WFS. So the only use of
Polyline is here to/from DBT models (read/prepare seeds), so rather use geojson
- LATER de json-ize SQL arrays
- convert from text : *_Id to uuid, numbers, bool, date, geo ; only if required by using macros instead of only SQL functions
- ? textify everything else

parameters :
- source : a dbt model (from ref() or source()), by default the current one minus _stg if any (NOT a WITH-defined alias, because it is always used in another _csv.sql model)
- column_models : used to guide parsing of values from text, and add missing columns as NULL if enabled (complete_columns_with_null)
the first column with a given name DEFINES EXACTLY the type of this column, others are converted to it (so must be compatible obviously)
So only the first column with a given name is kept?
- complete_columns_with_null
- wkt_rather_than_geojson
- date_formats : in the order of parsing preference, by default : 'YYYY-MM-DDTHH24:mi:ss.SSS' (RFC3339), 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'
- geo_pattern
- uuid_pattern
- geometry_column : to rename it

optional_column_model_TODO_or_types

NOT else error each UNION query must have the same number of columns :
        {% if debug %} -- TODO only if not ::text'd
          , {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name + '__src') }}
        {% endif %}
#}

{% macro from_csv(source, column_models=[], defined_columns_only=false, complete_columns_with_null=false, wkt_rather_than_geojson=false,
    date_formats=['YYYY-MM-DDTHH24:mi:ss.SSS', 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'],
    geo_pattern="geo.*", uuid_pattern="_Id|_Ref", geometry_column=None, def_from_source_mapping={}) %}

{% set source = source if source else ref(model.name | replace('_stg', '')) %}

{%- set cols = adapter.get_columns_in_relation(source) | list -%}
{%- set col_names = cols | map(attribute='name') | list -%}

{%- set all_col_names = [] -%}
{%- set all_def_cols = [] -%}
{# add columns that are in defs : #}
{% for column_model in column_models %}
  {% for col in adapter.get_columns_in_relation(column_model) | list %}
    {% if col.name not in all_col_names %}
      {% if all_def_cols.append(col) %}{% endif %}
      {% if all_col_names.append(col.name) %}{% endif %}
    {% endif %}
  {% endfor %}
{% endfor %}
{% if not defined_columns_only %}
  {# add columns that are not in defs : #}
  {% for col in cols %}
    {% if col.name not in all_col_names %}
      {% if all_def_cols.append(col) %}{% endif %}
      {% if all_col_names.append(col.name) %}{% endif %}
    {% endif %}
  {% endfor %}
{% endif %}

{%- set def_cols = all_def_cols if complete_columns_with_null else (all_def_cols | selectattr("name", "in", col_names) | list) -%}

-- from_csv {{ source }} - col_names : {{ col_names }} ; def_cols : {{ def_cols }} ; def_from_source_mapping : {{ def_from_source_mapping }} ; cols : {{ cols }}
select


    {% for def_col in def_cols %}
        {% set source_col_name = def_from_source_mapping[def_col.name] if def_from_source_mapping[def_col.name] and def_from_source_mapping[def_col.name] in col_names else def_col.name %}
        -- source_col_name : {{ source_col_name }} ; def_col.data_type : {{ def_col.data_type }} ; def_col.name :  {{ def_col.name }} ; test : {{ modules.re.match("geo.*", def_col.name, modules.re.IGNORECASE) }} ; source_col_name : {{ source_col_name }}

        {# TODO (but not for parsing from table) first of column_models must provide the type and therefore by 0-lined NOO ONLY IN dbt_utils.union()
        (required anyway to define EXACTLY the column type, so better than doing it in parsing macros, or here) #}
        {% if source_col_name not in col_names %}
          NULL::{% if def_col.is_number() %}numeric{% elif modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) %}geometry{% elif def_col.data_type == 'date' or def_col.data_type == 'timestamp' or def_col.data_type == 'timestamp with time zone' %}date{% elif def_col.data_type == 'boolean' %}boolean{% else %}text{% endif %} as {{ adapter.quote(def_col.name) }}
          {# NULL as {{ adapter.quote(def_col.name) }} #}
        {% else %}

        {% set source_col = cols | selectattr("name", "eq", source_col_name) | list | first %}

        {% if modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) %}
          {# TODO NOT IGNORECASE #}
          {{ fdr_appuiscommuns.to_geometry_or_null(source_col.name, source, wkt_rather_than_geojson=wkt_rather_than_geojson) }} as {{ adapter.quote(geometry_column if geometry_column else def_col.name) }}
        {# TODO from json : according to param, example data, meta ? NOO TODO json_to_array
        {% elif def_col.data_type == 'ARRAY' %}
          array_to_json({{ source }}.{{ adapter.quote(source_col.name) }}) as {{ adapter.quote(def_col.name) } #}
        {% elif modules.re.match(uuid_pattern, def_col.name) %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::uuid
        {% elif def_col.is_number() %}
          {{ fdr_appuiscommuns.to_numeric_or_null(source_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          -- {# {{ schema }}.fdr_appuiscommuns.to_numeric_or_null({{ source }}.{{ adapter.quote(def_col.name) }}) as {{ adapter.quote(def_col.name) }} #} -- or merely ::numeric ?
          --{{ source }}.{{ adapter.quote(def_col.name) }}::numeric -- NOT to_numeric_or_null else No function matches the given name and argument types.
        {% elif def_col.data_type == 'date' or def_col.data_type == 'timestamp' or def_col.data_type == 'timestamp with time zone' %}
          {{ schema }}.to_date_or_null({{ source }}.{{ adapter.quote(source_col.name) }}::text, {% for fmt in date_formats %}'{{ fmt }}'::text{% if not loop.last %}, {% endif %}{% endfor %}) as {{ adapter.quote(def_col.name) }}
        {% elif def_col.data_type == 'boolean' %}
          {{ fdr_appuiscommuns.to_boolean_or_null(source_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          --{{ schema }}.to_boolean_or_null({{ source }}.{{ adapter.quote(source_col.name) }}) as {{ adapter.quote(def_col.name) }} -- ? allows for 'oui'
        {# % elif def_col.is_string() %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name) }} -- in case it's NOT text ex. int4 because of dbt seed !
        #}
        {% else %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name) }}
        {% endif %}

        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    --, '{ "a":1, "b":"zz" }'::json as test
    from {{ source }}

{% endmacro %}