{#

Used to produce definition, as well as parse  tests.

Parsing of
- sources that are directly in the apcom types
- a priori (made-up), covering examples of the definition / interface.
- test _expected
Examples have to be **as representative** of all possible data as possible because they are also the basis of the definition.
For instance, for a commune INSEE id field, they should also include a non-integer value such as 2A035 (Belvédère-Campomoro).
Methodology :
1. copy the first line(s) from the specification document
2. add line(s) to contain further values for until they are covering for all columns
3. NB. examples specific to each source type are provided in _source_example along their implementation (for which they are covering)

TODO or _parsed, _definition_ ?
TODO can't be replaced by generic from_csv because is the actual definition, BUT could instead be by guided by metamodel !
{{ eaupot_reparations_from_csv(ref(model.name[:-4])) }}
#}

{% macro fdr_perimetre_from_csv(source_model=ref(model.name | replace('_stg', ''))) %}

{% set fieldPrefix = var('use_case_prefix') + 'percomp_' %}
{% set source_relation = source_model %}{# TODO rename #}
{% set source_alias = None %}{# 'source' TODO rename #}

select

       {#
       listing all fields for doc purpose, and not only those having to be transformed using {{ dbt_utils.star(def_model, except=[...
       because this is the actual definition of the standardized "echange" format
       #}

        ---- TODO '{{ source_relation }}' as "{{ fieldPrefix }}src_name", -- source name (else won't have it anymore once unified with other sources)
        ST_GeomFROMText("geom", 2154) as "geom", -- Multi Polygon, required

    from {{ source_model }}

{% endmacro %}