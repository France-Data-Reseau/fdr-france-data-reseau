{{
  config(
    materialized="view",
  )
}}

{% set source_sql_criteria %}
"FDR_CAS_USAGE" = 'sdirve' and "FDR_ROLE" = 'perimetre'
{% endset %}
{# rather than {% set FDR_SOURCE_NOM = 'perimetre_irve' %} #}

{% set has_dictionnaire_champs_valeurs = this.name.endswith('_dict') %}

{{ fdr_francedatareseau.fdr_source_union_from_criteria(source_sql_criteria,
    has_dictionnaire_champs_valeurs,
    this,
    def_model=ref('fdr_def_perimetre_definition')) }}