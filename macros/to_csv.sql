{#
version csv-isable (préparée pour export CSV) :
- geojson-ize geo fields (TODO convert sr if required) ; TODO keep original ?
Superset supports geohash, Polyline, geojson. But the only way Polyline is served on the way is
through geoserver WFS. So the only use of Polyline etc. is here to/from DBT models (read/prepare seeds).
- json-ize SQL arrays
- text-ify everything else besides numeric

parameters :
- source : it is advised to provide it explicitly so dbt can see its dependency.
A dbt model (from ref() or source()) (NOT a WITH-defined alias,
because it is always used in another _csv.sql model). By default is the current
model.name minus the (_wkt)_csv suffix.
- wkt_rather_than_geojson : for keeping Lambert93 or _expected (geojson loses precision) rather than
ckan. GeoJSON is by default because is the most obvious and useful (CKAN)
format for CSV.
#}

{% macro to_csv(source=none, wkt_rather_than_geojson=false, prefix_regex_to_remove=None) %}

{% set source = source if source else ref(model.name[:(-8 if wkt_rather_than_geojson else -4)]) %}
{% set cols = adapter.get_columns_in_relation(source) | list %}

select
    {% for col in cols %}
        {% if modules.re.match("geo.*", col.name, modules.re.IGNORECASE) %}
          {% if wkt_rather_than_geojson %}ST_AsText{% else %}ST_AsGeoJSON{% endif %}({{ source }}.{{ adapter.quote(col.name) }})
        {% elif col.data_type == 'ARRAY' %}
          array_to_json({{ source }}.{{ adapter.quote(col.name) }})
        {% elif col.is_string() or col.is_number() %}
          {{ source }}.{{ adapter.quote(col.name) }}
        -- elif date : ::text transforms date to rfc3339 by default i.e. 'YYYY-MM-DD"T"HH24:mi:ss.SSS' as required by apcom & eaupot
        -- NB. date has to be transformed to text in SQL (and types gotten from the source model), else dbt adapter raises ValueError: year 20222 is out of range,
        -- because of fromtimestamp explodes beyond 10000 and returns at least 1970 which is not an acceptable minimum https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp
        {% else %}
          {{ source }}.{{ adapter.quote(col.name) }}::text
        {% endif %}
        as {{ adapter.quote(modules.re.sub(prefix_regex_to_remove, '', col.name) if prefix_regex_to_remove else col.name) }}
        {% if not loop.last %},{% endif %}
        -- TODO NOT IGNORECASE
        -- col.data_type : {{ col.data_type }} ; col.name :  {{ col.name }} ; test : {{ modules.re.match("geo.*", col.name, modules.re.IGNORECASE) }}
    {% endfor %}
    --, '{ "a":1, "b":"zz" }'::json as test
    from {{ source }}

{% endmacro %}