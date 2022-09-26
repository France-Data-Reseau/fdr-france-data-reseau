{{
  config(
    materialized="view",
  )
}}

{% set sourceModel = ref('fdr_src_population_communes_parsed') %}

select
    {{ dbt_utils.star(sourceModel, except=[
          "Population municipale 2019"]) }},
    "Population municipale 2019"::numeric as "Population municipale 2019"
from {{ sourceModel }}