{#
Communes FR, with population (2019 and 2014 with additional demography stats)
incremental (table) so that DBT doesn't drop it (and dependent views along) at each run

Indexes :
on com_code for joins, and on geometry (4326) for ST_Contains

Fields : those of ref'd models plus
- geometry (shape, 4326) : indexed, also geometry_shape_2154 & geometry_center(_2154)
(?) not indexed on geometry_2154 because only used for computing distances so would not increase performance
(NB. moving _2154 and geo indexes to from_csv() wouldn't allow to make it a view because it would have to be added to imported table)

#}

{{
  config(
    materialized="incremental",
    unique_key=['com_code'],
    tags=['incremental'],
    indexes=[
        {'columns': ['com_code']},
        {'columns': ['geometry'], 'type': 'gist'},
    ],
  )
}}

{% set sourceModel = ref(this.name | replace('_std_', '_src_') ~ '_parsed') %}

select {{ dbt_utils.star(sourceModel, except=['geo_point_2d'], relation_alias="com",) }},
      --geometry as geometry_shape_4326, -- shape ; no use adding it without index
      ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326) as geometry_center, -- center ; _4326
      ST_Transform(geometry, 2154) as geometry_shape_2154, -- shape
      ST_Transform(ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326), 2154) as geometry_center_2154 -- center
      ,
      pop2019."Population municipale 2019",
      {{ dbt_utils.star(ref('fdr_src_demographie_communes_2014_typed'), relation_alias="comdemo",
            except=fdr_francedatareseau.list_import_fields()) }}

from {{ sourceModel }} com

    left join {{ ref('fdr_src_population_communes_typed') }} pop2019 -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        on com.com_code = pop2019."Code"

    left join {{ ref('fdr_src_demographie_communes_2014_typed') }} comdemo -- LEFT join sinon seulement les lignes qui ont une valeur !! TODO indicateur count pour le vérifier
        --on apcomsup."com_code" = demo."CODGEO"
        --on apcomsuparr."com_code__arr_u" = demo."CODGEO"
        on com.com_code = comdemo."CODGEO"

{% if is_incremental() %}
  where com.last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}