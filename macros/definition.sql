{#
Definition / interface
- with the proper column types (thanks to _example_stg),
- but without any data (to allow to use to define columns in sql ex. as first in union)

Because of these uses, it has to be a relation rather than a mere SQL alias etc.
#}


{% macro definition(source_example_model) %}
select
    {{ dbt_utils.star(source_example_model) }}
    from {{ source_example_model }}
    limit 0
{% endmacro %}