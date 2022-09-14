{#
Normalisation vers le modèle de données
à appliquer après le spécifique _from_csv, ou après le générique from_csv guidé par le modèle produit par _from_csv,
qui, eux, gèrent renommage et parsing générique
en service ET abandonnées (données et champs)

- OUI OU à chaque fois pour plus de concision et lisibilité select * (les champs en trop sont alors enlevés à la fin par la __definition) ?
#}

{% macro fdr_perimetre_translated(parsed_source_relation, src_priority=None) %}

{% set modelVersion ='_v3' %}

{% set containerUrl = 'http://' + 'datalake.francedatareseau.fr' %}
{% set typeUrlPrefix = containerUrl + '/dc/type/' %}
{% set type = 'fdr_perimetre_raw/nativesrc_extract' %} -- spécifique à la source ; _2021 ? from this file ? prefix:typeName ?
{% set type = 'fdr_perimetre' %} -- _2021 ? from this file ? prefix:typeName ?
{% set fdr_namespace = 'perimetre.' + var('fdr_namespace') %} -- ?
{% set typeName = 'Perimetre' %}
{% set sourcePrefix = 'fdr' %} -- ?
{% set prefix = var('use_case_prefix') + 'percomp' %} -- ?
{% set sourceFieldPrefix = sourcePrefix + ':' %}
{% set sourceFieldPrefix = sourcePrefix + '_' %}
{% set fieldPrefix = prefix + ':' %}
{% set fieldPrefix = prefix + '_' %}
{% set idUrlPrefix = typeUrlPrefix + type + '/' %}

with import_parsed as (

    select * from {{ parsed_source_relation }}
    {% if var('limit', 0) > 0 %}
    LIMIT {{ var('limit') }}
    {% endif %}

{#
rename and generic parsing is rather done
- in specific _from_csv
- in generic from_csv (called by fdr_source_union), which is guided by the previous one
#}

), specific_parsed as (

    -- handle official "echange" fields that are not fully perfect :
    select
        --*,
        {{ dbt_utils.star(parsed_source_relation) }},

        1 as "{{ fieldPrefix }}src_id" -- "{{ fieldPrefix }}fid"

    from import_parsed

), with_generic_fields as (

    {{ fdr_appuiscommuns.add_generic_fields('specific_parsed', fieldPrefix, fdr_namespace, src_priority) }}

), specific_renamed as (

    select
        *--,

        --"{{ fieldPrefix }}geom" as "{{ fieldPrefix }}geometry" NOO not possible here

    from with_generic_fields

)

select * from specific_renamed

{% endmacro %}