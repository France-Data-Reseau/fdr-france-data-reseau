{#
Vue utilisée par Superset pour les indicateurs Gros nombres du tableau de bord Eclairage public
#}

{{
  config(
    materialized="view",
  )
}}

{% set sourceModel = ref("fdr_std_population_communes_typed") %}

SELECT count(reference) as nombre_point_lumineux, sum(puissance) as puissance_totale, gestionnaire_title, upper(unaccent("Libellé")) as est_dans_commune_com_nom, "Population municipale 2019"
from stellio.pointlumineux_eclairage_public
inner join {{ sourceModel }} on "Code" = insee::TEXT
group by gestionnaire_title, "Libellé", "Population municipale 2019"