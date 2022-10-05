{#
single step (identical) deduplication
(rather than 1. list dups couples 2. process them according to any rules (CTE on their values AND possibly enriched by human experts choices) 3. loop)

89s on apcom osm sup with no index (!)
TODO obviously faster if indexed on id_fields ?

example :
{{ dedupe('id_deduped', id_fields=['"geometry"']) }}

parameters :
- source_name : a valid SQL alias or relation reference
- id_fields : to dedupe on
- order_by : to resolve dedupe in addition to id fields
- where : filter ex. to remove those without id fields or get a subset

The best method varies depending on the database server.
#}

{% macro dedupe(source_name, id_fields, order_by=None, where=None) %}

{#
Postgresql :

inspired by https://stackoverflow.com/questions/61424956/postgresql-remove-duplicates-by-group-by
using "distinct on"
(or for more control on order ex. first like "limit 1" on bigquery, "partition by" window function)

other ones :
ctid if no id fields, but for performance they should have a primary key so ctid not possible (on solutions, by order of performance : where, window function, distinct on ??) https://stackoverflow.com/questions/53722174/most-efficient-way-to-remove-duplicates-postgres
group by having, where : https://stackoverflow.com/questions/39928704/postgresql-removing-duplicates
group by, window function, where : https://stackoverflow.com/questions/2230295/whats-the-best-way-to-dedupe-a-table
#}

  {% set id_field_csv = id_fields | join(", ") if id_fields is iterable else id_fields %}
  {% set order_by = id_field_csv ~ ", " ~ order_by if order_by else id_field_csv %}
  SELECT DISTINCT ON ({{ id_field_csv }}) {{ source_name }}.*
  FROM {{ source_name }}
  {% if where %}
  WHERE {{ where }}
  {% endif %}
  ORDER BY {{ order_by }}
  
{#
BigQuery : using aggregation.

inspired by https://github.com/dbt-labs/dbt-utils/issues/335

single pass dedup, rules (& choices) are limited to what can be provided in array_agg

It is more performant (can handle more rows, not necessarily faster) than using row_count().
Adding the LIMIT 1 allows BQ to vastly reduce the dataset it needs to calculate the query.

Pass in a source, so that this can be used also as a separate CTE.

Tried to make it work on postgresql, but failed :
(returns a single array column or "subscripting" error, see :
https://www.mail-archive.com/pgsql-hackers@lists.postgresql.org/msg75908.html
https://stackoverflow.com/questions/6960247/postgresql-convert-array-returned-from-function-to-columns
)

      order by case when MyDate is null then 1 else 0 end
      FILTER (WHERE {{ filter_where | "" }} order by "updated" desc limit 1)
      order by "updated" desc limit 1
examples :
dedupe by geometry, choosing line with highest (assuming newest ex. in OSM) source id : 
dedupe_agg("apcom_support_aerien_translation", group_by="geometry", order_by='"appuiscommunssupp__src_id" desc')
dedupe by geometry, choosing latest : 
dedupe_agg("apcom_support_aerien_translation", group_by="geometry", order_by='"updated" desc')

  select
    best_line.*
    -- best_line.arr[1] -- ERROR: cannot subscript type appuiscommuns_test.appuiscommuns_supportaerien because it does not support subscripting
  from (
    select
      (array_agg (
        src
        {% if order_by %}
        order by {{ order_by }}
        {% endif %}
      )
      
      {% if where %}
      FILTER (WHERE {{ filter_where }})
      {% endif %}
      
      )[1]
    from {{ source_name }} src
    group by {{ group_by }}
  ) best_line

#}

{% endmacro %}