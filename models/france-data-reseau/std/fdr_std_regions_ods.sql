{#
Regions FR
incremental (table) so that DBT doesn't drop it (and dependent views along) at each run (filter not needed for DBT performance)

Indexes :
on geometry (4326) for ST_Contains (on reg_code not need for join performance)

Fields : those of ref'd models plus
- geometry (shape, 4326) : indexed, also geometry_shape_2154 & geometry_center(_2154)
(?) not indexed on geometry_2154 because only used for computing distances so would not increase performance
(NB. moving _2154 and geo indexes to from_csv() wouldn't allow to make it a view because it would have to be added to imported table)

- geometry (shape, 4326) : indexed, also geometry_shape_2154 & geometry_center(_2154)
- geo_point_2d from _float8 to geometry, no index

(?) not indexed on geometry_2154 because only used for computing distances so would not increase performance
#}

{{
  config(
    materialized="incremental",
    unique_key=['reg_code'],
    tags=['incremental'],
    indexes=[{'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set sourceModel = ref(this.name | replace('_std_', '_src_') ~ '_parsed') %}

select {{ dbt_utils.star(sourceModel, except=["geo_point_2d"]) }},
      --geometry as geometry_shape_4326, -- shape ; no use adding it without an index
      ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326) as geometry_center, -- center ; _4326
      ST_Transform(geometry, 2154) as geometry_shape_2154, -- shape
      ST_Transform(ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326), 2154) as geometry_center_2154 -- center
from {{ sourceModel }}

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}