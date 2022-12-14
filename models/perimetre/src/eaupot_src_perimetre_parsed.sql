{{
  config(
    materialized="view",
  )
}}

-- TODO patch FDR_ROLE to 'perimetre' in eaupotable metadata
{% set source_sql_criteria %}
"FDR_CAS_USAGE" = 'eaupotable' and "FDR_SOURCE_NOM" = 'perimetre'
{% endset %}

{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_criteria(source_sql_criteria,
    has_dictionnaire_champs_valeurs,
    this,
    def_model=ref('fdr_def_perimetre_definition')) }}