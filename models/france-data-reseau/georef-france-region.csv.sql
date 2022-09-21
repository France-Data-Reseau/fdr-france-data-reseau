{#
geo indexed version ("staging" version in the words of DBT) of the CKAN imported CSV file.
TODO move it in the fdr_francedatareseau DBT project.
NB. might be auto generated.

indexes are not required for performance : (BUT probably only because small data, contrary to commune) :
indexes=[{'columns': ['geo_shape_4326'], 'type': 'gist'},]

geojson version could be kept to not to have to regenerate it for CSV output (TODO generate it everywhere, as in _csv.sql) :
, except=[
      "Geo Point",
      "Geo Shape"]
#}

{{
  config(
    enabled=target.name.endswith('_ckan'),
    materialized="table",
    
  )
}}

select
    {{ dbt_utils.star(source('france-data-reseau', 'georef-france-region_old.csv')) }}
    --, ST_PointFromText('POINT(' || replace(c."Geo Point", ',', ' ') || ')', 4326) as geo_point_4326,
    --ST_Transform(ST_GeomFromGeoJSON(c."Geo Shape"), 4326) as geo_shape_4326
from {{ source('france-data-reseau', 'georef-france-region_old.csv') }} c