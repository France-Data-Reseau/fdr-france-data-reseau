{{
  config(
    materialized="view",
  )
}}

{% set sourceModel = ref(this.name | replace('_typed', '_parsed')) %}

select
    {{ dbt_utils.star(sourceModel, except=[
          "Population"]) }},
    "Population"::numeric as "Population"
from {{ sourceModel }}