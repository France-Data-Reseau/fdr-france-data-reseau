{#
Used to define fal on it to publish it

    schema=source('fdr_import', 'fdr_import_resource').schema
NOO "custom schema" creates ex. appuiscommuns_appuiscommuns schema unless generate_schema_name() changed :
https://docs.getdbt.com/docs/building-a-dbt-project/building-models/using-custom-schemas
#}

{{
  config(
    materialized="view",
  )
}}

select * from {{ source('fdr_import', 'fdr_import_resource') }}