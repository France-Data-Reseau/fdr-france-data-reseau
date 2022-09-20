{#
- geometry (shape) : indexed, renamed from wkb_geometry, converted to 2154
- geo_point_2d from _float8 to geometry, no index
#}

{{
  config(
    materialized="table",
    indexes=[{'columns': ['geometry'], 'type': 'gist'},]
  )
}}

{% set use_case_prefix = var('use_case_prefix') %}
{% set FDR_SOURCE_NOM = this.name | replace(use_case_prefix ~ '_src_', '') | replace('_parsed', '') | replace('_dict', '') %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

with imported as (
select * from {{ ref('fdr_src_regions_ods_parsed') }}
)
select {{ dbt_utils.star(ref('fdr_src_regions_ods_parsed'), except=["geo_point_2d"]) }},
      ST_Transform(wkb_geometry, 2154) as geometry, -- shape
      ST_Transform(ST_SetSRID(ST_MakePoint(geo_point_2d[2], geo_point_2d[1]), 4326), 2154) as geo_point_2d -- center
from {{ ref('fdr_src_regions_ods_parsed') }}