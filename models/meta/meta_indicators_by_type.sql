
{#
     | selectattr("resource_type", "equalto", "model")
     
    , '{{ node.tags | select("equalto", "definition") | join() | replace("definition", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "dictionary") | join() | replace("dictionary", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "normalization") | join() | replace("normalization", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "sample") | join() | replace("sample", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "expected") | join() | replace("expected", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "unification") | join() | replace("unification", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "enriched") | join() | replace("enriched", "1") | default("0", true) }}'::integer,
    '{{ node.tags | select("equalto", "indicators") | join() | replace("indicators", "1") | default("0", true) }}'::integer,
    
      is_definition, is_dictionary,
      is_normalization/*transformation*/, is_sample/*extract*/, /*is_test unittest*/is_expected,
      is_unification, is_enriched,
      is_indicators/*agg by*/,
      
      , is_definition, is_dictionary,
      is_normalization/*transformation*/, is_sample/*extract*/, /*is_test unittest*/is_expected,
      is_unification, is_enriched,
      is_indicators,
#}

-- depends_on: {{ ref('meta_indicators') }}
{% if execute %}
{% set tags = ["definition", "dictionary", "normalization", "sample", "expected", "unification", "enriched", "indicators"] %}
{% set use_case = 'appuiscommuns' %}
with grouped as (
  select
      use_case, type,
      {% for tag in tags %}
      sum({{ tag }}) as {{ tag }}
      {% if not loop.last %}
        ,
      {% endif %}
      {% endfor %}
      
  from {{ ref('meta_indicators') }} group by use_case, type
)

select * from grouped
{% endif %}

/*
  Example output
---------------------------------------------------------------
...
*/