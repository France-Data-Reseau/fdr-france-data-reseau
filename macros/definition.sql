{#
Definition / interface
- with the proper column types. Get the from _example(_stg), and this one from a DBT seed, where the first line is a
dummy one where all (non-int4) NULL valued fields are set so that DBT doesn't recognize them as int4.
- but without any data (to allow to use to define columns in sql ex. as first in union)

Because of these uses, it has to be a relation rather than a mere SQL alias etc.
#}


{% macro definition(source_example_model) %}
select
    {{ dbt_utils.star(source_example_model) }}
    from {{ source_example_model }}
    limit 0
{% endmacro %}