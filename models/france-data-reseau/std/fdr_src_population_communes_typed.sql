{#
Population des communes

Now provided in fdr_std_communes_ods

TODO rename _std_

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

{% set sourceModel = ref(this.name | replace('_typed', '_parsed')) %}

select
    {{ dbt_utils.star(sourceModel, except=[
          "Population municipale 2019"]) }},
    "Population municipale 2019"::numeric as "Population municipale 2019"
from {{ sourceModel }}

{% if is_incremental() %}
  where last_changed > (select max(last_changed) from {{ this }})
{% endif %}