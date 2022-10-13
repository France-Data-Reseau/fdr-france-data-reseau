{#
Population des communes

Now also provided in fdr_std_communes_ods

TODO remove doc about stellio.point_lumineux_indicateurs_habitants_eclairage_public once Superset uses rather
the DBT managed one (eclpub_kpi_point_lumineux_indicateurs_habitants)

WARNING made incremental, else DBT cascade drops dependent views created in Superset
NB. alternatives : put them in DBT (!), enabled=false, or make them as tables (filled by Nifi)
see https://github.com/dbt-labs/dbt-core/issues/2185

i.e. only created if does not yet exist, though filled everytime (so must have a unique_key)
(TODO LATER macro that empties them on-run-start)
bonus : if run with is_incremental, only fills the ones with a newer last_changed

said Superset views :

create view stellio.point_lumineux_indicateurs_habitants_eclairage_public as (
    SELECT count(reference) as nombre_point_lumineux, sum(puissance) as puissance_totale, gestionnaire_title, upper(unaccent("Libellé")) as est_dans_commune_com_nom, "Population municipale 2019"
    from stellio.pointlumineux_eclairage_public
    inner join "france-data-reseau"."fdr_src_population_communes_typed" on "Code" = insee::TEXT
    group by gestionnaire_title, "Libellé", "Population municipale 2019"
);
#}

{{
  config(
    materialized="incremental",
    unique_key='"Code"',
    tags=['incremental'],
    indexes=[
        {'columns': ['"Code"']},
    ],
  )
}}

{% set sourceModel = ref(this.name | replace('_std_', '_src_') | replace('_typed', '_parsed')) %}

select
    {{ dbt_utils.star(sourceModel, except=[
          "Population municipale 2019"]) }},
    "Population municipale 2019"::numeric as "Population municipale 2019"
from {{ sourceModel }}

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}