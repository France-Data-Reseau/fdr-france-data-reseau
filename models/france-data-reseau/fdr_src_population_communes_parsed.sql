{{
  config(
    materialized="view",
  )
}}

{% set use_case_prefix = var('use_case_prefix') %}
{% set FDR_SOURCE_NOM = this.name | replace(use_case_prefix ~ '_src_', '') | replace('_parsed', '') | replace('_dict', '') %}
{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_name(FDR_SOURCE_NOM,
    has_dictionnaire_champs_valeurs,
    this) }}