{{
  config(
    materialized="view",
  )
}}

{% set use_case_prefix = 'sdirve' %}
{#% set FDR_SOURCE_NOM = this.name | replace(use_case_prefix ~ '_src_', '') | replace('_parsed', '') | replace('_dict', '') %#}
{% set FDR_SOURCE_NOM = 'perimetre_irve' %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_name(FDR_SOURCE_NOM,
    has_dictionnaire_champs_valeurs,
    this,
    def_model=ref('fdr_def_perimetre_definition'),
    FDR_CAS_USAGE='sdirve') }}