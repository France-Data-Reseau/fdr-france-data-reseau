
{{
  config(
    materialized="view",
  )
}}

{% set source_relation = source('fdr_ckan', 'fdr_ckan_resource_synced') %}

select
    {{ dbt_utils.star(source_relation, except=['dsid', 'orgid']) }},
    dsid AS ds_id,
    orgid AS org_id
from {{ source_relation }}
