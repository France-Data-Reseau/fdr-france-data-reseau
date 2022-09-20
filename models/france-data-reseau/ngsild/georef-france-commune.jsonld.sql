{#

TODO reste :
json parse de location
"ept_code": NaN,

#}

{{
  config(
    enabled=not target.name.endswith('_stellio'),
    materialized="view"
  )
}}

{% set source_relation = ref('georef-france-commune.csv') %}
{% set source_alias = 'csvized' %}

with csvized as (
{{ to_csv(source_relation) }}
)

select
    'urn:ngsi-ld:fdr:Commune:cog_ods_000000001_' || com_code as id,
    'Commune' as type,
    'https://raw.githubusercontent.com/france-data-reseau/fdr-data-models/master/fdr/jsonld-contexts/commune-compound.jsonld' as "@context", -- OK or [] required ?
    geo_shape_4326 as location,

    "year"::text as "year",
    {#{{ fdr_francedatareseau.to_date_or_null('year', source_relation, ['YYYY'], source_alias) }}::date as "year", -- 2021-10-01 Date de validité de l'indicateur #}
    {{ fdr_francedatareseau.to_boolean_or_null('com_in_ctu', source_relation, source_alias) }} as "com_in_ctu", -- true
    {{ fdr_francedatareseau.to_boolean_or_null('com_is_mountain_area', source_relation, source_alias) }} as "com_is_mountain_area", -- true

    -- all other are string (codes should not be numbers) :
    {% for col_name in adapter.get_columns_in_relation(source_relation) | list | map(attribute='name') | reject("in", [
                                                                                      "_id", "_full_text",
                                                                                      "geo_point_4326", "geo_shape_4326",
                                                                                      "year", "com_in_ctu", "com_is_mountain_area"]) %}
        {#{{ source_relation }}.#}{{ adapter.quote(col_name) }}::text as {{ adapter.quote(col_name) }}{% if not loop.last %},{% endif %}
    {% endfor %}

    {#{ dbt_utils.star(ref('georef-france-commune.csv'), except=[
      "_id", "_full_text",
      "geo_point_4326", "geo_shape_4326",
      "year", "com_in_ctu", "com_is_mountain_area"]) }#}
from csvized