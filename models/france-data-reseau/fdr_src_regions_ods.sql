{#
- geometry (shape) : indexed, renamed from wkb_geometry, converted to 2154
- geo_point_2d from _float8 to geometry, no index

(?) not indexed on geometry_2154 because only used for computing distances so would not increase performance
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set sourceModel = ref(this.name ~ '_parsed') %}

select {{ dbt_utils.star(sourceModel, except=["geo_point_2d"]) }},
      --geometry as geometry_shape_4326, -- shape ; no use adding it without an index
      ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326) as geometry_center, -- center ; _4326
      ST_Transform(geometry, 2154) as geometry_shape_2154, -- shape
      ST_Transform(ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326), 2154) as geometry_center_2154 -- center
from {{ sourceModel }}