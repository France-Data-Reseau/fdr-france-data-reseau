{#
(rather than as dedicated dbt models ? alas in both cases source requires yaml def...)
il manquait BEGIN; COMMIT; pour que create view dans macro marche !! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
#}
{% macro create_views_per_data_owner(FDR_CAS_USAGE=var('FDR_CAS_USAGE'), relations=var('create_views_per_data_owner_relations')) %}
{{ log("create_views_per_data_owner start", info=True) }}

{% set fdr_import_resource_model = source("fdr_import", "fdr_import_resource")  %}
{% set fdr_data_owners = fdr_appuiscommuns.fdr_data_owners(FDR_CAS_USAGE, fdr_import_resource_model) %}
{% do log("create_views_per_data_owner fdr_data_owners res " ~ fdr_data_owners, info=True) %}{# see https://docs.getdbt.com/reference/dbt-jinja-functions/run_query https://agate.readthedocs.io/en/latest/api/table.html #}

{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}
{% for relation in relations %}
    {% for fdr_data_owner_row in fdr_data_owners.rows %}
        {{ log("create_views_per_data_owner start... ", info=True) }}
        {% set sql %}
        BEGIN;
        DROP VIEW IF EXISTS {{ target.schema }}."{{ relation }}_{{ fdr_data_owner_row['data_owner_id'] }}";
        create view {{ target.schema }}."{{ relation }}_{{ fdr_data_owner_row['data_owner_id'] }}" as
        select *
        from {{ target.schema }}."{{ relation }}"
        where data_owner_id = '{{ fdr_data_owner_row["data_owner_id"] }}'
        ;
        COMMIT; -- else does not create view ! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
        {% endset %}
        {% do log("create_views_per_data_owner fdr_data_owner sql " ~ sql, info=True) %}
        {% do run_query(sql) %}
        {% do log("create_views_per_data_owner fdr_data_owner done", info=True) %}
    {% endfor %}
{% endfor %}
{% endif %}
{{ log("create_views_per_data_owner end") }}
{% endmacro %}

{% macro fdr_data_owners(FDR_CAS_USAGE, fdr_import_resource_model=source("fdr_import", "fdr_import_resource")) %}
{% set sql %}
select data_owner_id, min(org_name) as data_owner_label, count(*) as import_count -- TODO org_title ?!
from "france-data-reseau".fdr_import_resource s
where "FDR_CAS_USAGE" = '{{ FDR_CAS_USAGE }}'
--and status = 'success' and "FDR_TARGET" <> 'archive' -- better than no errors or "FDR_SOURCE_NOM" is not null
group by data_owner_id; -- TODO schema _test
{% endset %}
{% do log("fdr_data_owners sql " ~ sql, info=True) %}
{% set fdr_data_owners = run_query(sql) %}
{% do log("fdr_data_owners res " ~ fdr_data_owners, info=True) %}{# see https://docs.getdbt.com/reference/dbt-jinja-functions/run_query https://agate.readthedocs.io/en/latest/api/table.html #}
{{ return(fdr_data_owners) }}
{% endmacro %}