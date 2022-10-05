{#
only transforms SR if already is data_type geo* in the source model, else applies CTE to parse and transform

-- {{  col.data_type if col else 'no col' }}
#}


{% macro to_geometry_or_null(column_name, source, source_alias=None, wkt_rather_than_geojson=false, srid=2154) %}
{#-- to_geometry_or_null source cols : {{ adapter.get_columns_in_relation(source) }} #}
{% set col = adapter.get_columns_in_relation(source) | selectattr("name", "eq", column_name) | list | first %}
{# checking whether PostGIS types : TODO LATER better, using type column_override like in dbt-utils.union() ? #}
-- to_geometry_or_null source col : col ; USER-DEFINED : {{ col.data_type == 'USER-DEFINED' }} ; wkt_rather_than_geojson : {{ wkt_rather_than_geojson }}
{% if col.data_type == 'USER-DEFINED' %}{# non CSV but database case ex. geopackage #}
ST_Transform({{ source }}.{{ adapter.quote(col.name) }}, {{ srid }})
{% elif not wkt_rather_than_geojson %}
--ST_Transform(ST_GeomFromGeoJSON({{ source }}.{{ adapter.quote(col.name) }}), {{ srid }})
"{{ schema }}".geojson_to_geometry_or_null({{ source }}.{{ adapter.quote(col.name) }}, {{ srid }})
{% else %}
--ST_GeomFROMText({{ source }}.{{ adapter.quote(col.name) }}, {{ srid }})
"{{ schema }}".wkt_to_geometry_or_null({{ source }}.{{ adapter.quote(col.name) }}, {{ srid }})
{% endif %}
--NOO ST_PointFromText('POINT(' || replace(c.geo_point_2d, ',', ' ') || ')', 4326) as geo_point_4326,
{% endmacro %}