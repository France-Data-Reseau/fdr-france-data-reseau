{{
  config(
    materialized="view",
  )
}}

-- TODO patch FDR_ROLE to 'perimetre' in eaupotable metadata
{% set source_sql_criteria %}
"FDR_ROLE" = 'perimetre' or "FDR_SOURCE_NOM" = 'perimetre'
{% endset %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_criteria(source_sql_criteria, has_dictionnaire_champs_valeurs,
    this,
    forced_source_nom='perimetre_all',
    def_model=ref('fdr_def_perimetre_definition')) }}