
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

{% if execute %}
{% set tags = ["definition", "dictionary", "normalization", "sample", "expected", "unification", "enriched", "indicators"] %}
{% set use_case = 'appuiscommuns' %}
with nodes as (

  select * from (values
  {% for node in graph.nodes.values() | selectattr("schema", "equalto", this.schema) %}
    (
    '{{ node.name }}', '{{ use_case }}',
    {% for tag in tags %}
      {% if modules.re.match(".+_" + tag + ".*", node.name) %}
        1,
      {% else %}
        {{ node.tags | select("equalto", tag) | join() | replace(tag, "1") | default("0", true) }},
      {% endif %}
    {% endfor %}
    '{{ node | tojson() | replace("'", "''") }}'::json
    )
    {% if not loop.last %}
      ,
    {% endif %}
  {% endfor %}
  ) x (name, use_case,
      {% for tag in tags %}
      {{ tag }},
      {% endfor %}
      node_json)
), typed as (
  select
      name, use_case,
      case "definition" = 1 or "normalization" = 1 or "sample" = 1 or "expected" = 1 or "unification" = 1 or "enriched" = 1 when TRUE then (regexp_match(name, '{{ use_case }}_([^_]*)'))[1] else NULL end as type,
      {% for tag in tags %}
      {{ tag }},
      {% endfor %}
      node_json
      
  from nodes
)

select * from typed
{% endif %}

/*
  Example output
---------------------------------------------------------------
...
*/