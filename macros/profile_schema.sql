{#
Simple data profiler based on (ported from BigQuery to PostgreSQL) :
https://rittmananalytics.com/blog/2020/6/4/column-level-data-profiling-for-google-bigquery-datasets-using-dbt

Computes simple indicators on its values for each field of each table of the provided schema.

TODO :
- try to rather use a more supported solution (*)
- patch avg (does not work when some nulls...)

(*) other solutions :
- https://github.com/data-mie (from DBT Hub) : alas error
dbt run-operation print_profile_schema --args '{"schema": "app_public", "relation_name": "pipe_material"}'
18:38:30  Running with dbt=1.0.1
18:38:31  Encountered an error while running operation: Database Error
  UNION types text and numeric cannot be matched
  LINE 257:           avg("sort_order") as avg,

cd integration_tests
put a profile in dbt_project.yml (or create the one in profiles.yml and use it)
dbt seed
dbt --debug run-operation print_profile_docs --args '{"relation_name": "test_data"}'
vue profile, table profile_over_time


#}

{%- macro profile_schema(table_schema) -%}

{# commented because aften user has no rights to create such schema {{target.schema}}_profiles
{{ config(schema='profiles') }}
#}

{% set not_null_profile_threshold_pct = ".9" %}
{% set unique_profile_threshold_pct = ".9" %}

{% set tables = dbt_utils.get_relations_by_prefix(table_schema, '') %}

SELECT column_stats.table_catalog,
       column_stats.table_schema,
       column_stats.table_name,
       column_stats.column_name,
       case when column_metadata.is_nullable = 'YES' then false else true end as is_not_nullable_column,
       case when column_stats.pct_not_null > {{ not_null_profile_threshold_pct }} then true else false end as is_recommended_not_nullable_column,

       column_stats._nulls as count_nulls,
       column_stats._non_nulls as count_not_nulls,
       column_stats.pct_not_null as pct_not_null,
       column_stats.table_rows,
       column_stats.count_distinct_values,
       column_stats.pct_unique,
       case when column_stats.pct_unique >= {{ unique_profile_threshold_pct }} then true else false end as is_recommended_unique_column,

       {{ dbt_utils.star(from=source('database_metadata', 'columns'), except=[
           "table_catalog",
           "table_schema",
           "table_name",
           "column_name",
           "is_nullable",
           
           "is_generated",
           "generation_expression",
           "is_updatable"]) }},
{#
       column_metadata.* EXCEPT (table_catalog,
                       table_schema,
                       table_name,
                       column_name,
                       is_nullable),
#}
        _min_value, _max_value, _avg_value, _most_frequent_value, _min_length, _max_length, _avr_length
{#
       column_stats.* EXCEPT (table_catalog,
                              table_schema,
                              table_name,
                              column_name,
                              _nulls,
                              _non_nulls,
                              pct_not_null,
                              table_rows,
                              pct_unique,
                              count_distinct_values)
#}
FROM
(
{% for table_ in tables %}
  SELECT *
  FROM
(
  WITH
    table__ AS (SELECT * FROM {{ table_ }} ),
    table_as_json AS (SELECT REGEXP_REPLACE(row_to_json(t)::text, '^{|}$', '') AS row_ FROM table__ AS t ),
    pairs AS (SELECT REPLACE(column_name, '"', '') AS column_name, case when CAST(column_value AS text)='null' then NULL else column_value end AS column_value
              FROM table_as_json, (select ((STRING_TO_ARRAY(z, ':'))[1]) AS column_name,((STRING_TO_ARRAY(z, ':'))[2]) AS column_value from (select UNNEST(STRING_TO_ARRAY(row_, ',"')) AS z from table_as_json) zz) zzz ),
    profile AS (
    SELECT
      (STRING_TO_ARRAY(replace('{{ table_ }}','`',''),'.' ))[1] as table_catalog,
      (STRING_TO_ARRAY(replace('{{ table_ }}','`',''),'.' ))[2] as table_schema,
      (STRING_TO_ARRAY(replace('{{ table_ }}','`',''),'.' ))[3] as table_name,
      column_name,
      COUNT(*) AS table_rows,
      COUNT(DISTINCT column_value) AS count_distinct_values,
      case when COUNT(*) = 0 then 0 else (COUNT(DISTINCT column_value) / COUNT(*)) end AS pct_unique,
      COUNT(column_value) FILTER (WHERE column_value IS NULL) AS _nulls,
      COUNT(column_value) FILTER (WHERE column_value IS NOT NULL) AS _non_nulls,
      case when COUNT(*) = 0 then 0 else (COUNT(column_value) FILTER (WHERE column_value IS NOT NULL) / COUNT(*)) end AS pct_not_null,
      min(column_value) as _min_value,
      max(column_value) as _max_value,
      avg(to_decimal_or_null(column_value)) FILTER (WHERE to_decimal_or_null(column_value) IS NOT NULL) as _avg_value,
      mode() WITHIN GROUP (ORDER BY column_value DESC) AS _most_frequent_value,
      MIN(LENGTH(CAST(column_value AS text))) AS _min_length,
      MAX(LENGTH(CAST(column_value AS text))) AS _max_length,
      ROUND(AVG(LENGTH(CAST(column_value AS text)))) AS _avr_length
    FROM
      pairs
    WHERE
      column_name <> ''
      AND column_name NOT LIKE '%-%'
    GROUP BY
      column_name
    ORDER BY
      column_name)
  SELECT
    *
  FROM
    profile) {{ table_|replace("\".\"" + table_schema + "\".\"", "__" + table_schema + "__profile_") }}
{%- if not loop.last %}
    UNION ALL
{%- endif %}
{% endfor %}
) column_stats
LEFT OUTER JOIN
(
  SELECT
    {{ dbt_utils.star(from=source('database_metadata', 'columns'), except=[
       "is_generated",
       "generation_expression",
       "is_stored",
       "is_updatable"]) }}
{#
    * EXCEPT
      (is_generated,
       generation_expression,
       is_stored,
       is_updatable)
#}
  FROM
    INFORMATION_SCHEMA.COLUMNS
) column_metadata
ON  column_stats.table_catalog = column_metadata.table_catalog
AND column_stats.table_schema = column_metadata.table_schema
AND column_stats.table_name = column_metadata.table_name
AND column_stats.column_name = column_metadata.column_name

{%- endmacro -%}