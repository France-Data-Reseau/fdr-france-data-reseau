{#
Vue utilisée par Superset pour les indicateurs Gros nombres du tableau de bord Eclairage public
#}

{{
  config(
    materialized="view",
  )
}}

{% set sourceModel = ref("fdr_std_population_communes_typed") %}

SELECT count(pointlumineux_eclairage_public.reference) AS nombre_point_lumineux,
    sum(pointlumineux_eclairage_public.puissance) AS puissance_totale,
    pointlumineux_eclairage_public.gestionnaire_title,
    pointlumineux_eclairage_public.est_dans_commune_com_nom,
    fdr_src_population_communes_typed."Population municipale 2019"
   FROM stellio.pointlumineux_eclairage_public
     JOIN "france-data-reseau".fdr_src_population_communes_typed ON fdr_src_population_communes_typed."Code" = pointlumineux_eclairage_public.insee
  GROUP BY pointlumineux_eclairage_public.gestionnaire_title, pointlumineux_eclairage_public.est_dans_commune_com_nom, fdr_src_population_communes_typed."Population municipale 2019"
