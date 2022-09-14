{#
Provides metrics over import results.
Also used to define fal on it to publish it.

    schema=source('fdr_import', 'fdr_import_resource').schema
NOO "custom schema" creates ex. appuiscommuns_appuiscommuns schema unless generate_schema_name() changed :
https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas
#}

{{
  config(
    materialized="view",
  )
}}

with import_resource as (
  select * from {{ source('fdr_import', 'fdr_import_resource') }}

), with_metrics as (
    SELECT *,
        (case when status = 'success' then 1 else 0 end ) As success,
        (case when status = 'skipped' then 1 else 0 end ) As skipped,
        (case when status = 'error' then 1 else 0 end ) As error
    from import_resource
)
select * from with_metrics