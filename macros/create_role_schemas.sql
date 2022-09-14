{#

Creates database roles and schemas
for all base schemas provided in schema dbt var, and schema suffixes in schema_suffixes dbt var.
Must be run as database admin, to init or update the FDR structure of the
FDR datalake database.

TODO TODO rather adapter.create_schema() https://docs.getdbt.com/reference/dbt-jinja-functions/adapter#create_schema

cd fdr-france-data-reseau/
# create use case roles and (shared, not personal) schemas with rights :
#dbt run-operation create_udfs --target prod_sync
dbt run-operation create_roles_schemas --target prod_sync

TODO ? --args '{ schemas: ["appuiscommuns", "sdirve", "eaupotable", "eclairage_public"] }'
#}

{% macro create_roles_schemas(schemas = var("schemas") if var("schemas") else ['appuiscommuns', 'sdirve', 'eaupotable', 'eclairage_public', 'france-data-reseau'],
        schema_suffixes = var("schema_suffixes") if var("schema_suffixes") else ['', '_test']) %}

{# on-run-start is not called by run-operation so let's define UDFs here rather than in on-run-start create_udfs() : #}
{{ create_role_schema_udfs() }}

{% for schema in schemas %}
    {% for schema_suffix in schema_suffixes %}
        {{ create_role_schema(schema ~ schema_suffix) }}
    {% endfor %}
{% endfor %}
{% endmacro %}


{% macro create_role_schema(name) %}
--CREATE ROLE "datactivist";
--CREATE SCHEMA AUTHORIZATION "datactivist";
--alter default privileges in schema "datactivist" grant all privileges on tables to "datactivist";
--CREATE ROLE "{{ name }}";

{% set sql %}
select public.create_role_if_not_exists('{{ name }}');
CREATE SCHEMA IF NOT EXISTS AUTHORIZATION "{{ name }}";
alter default privileges in schema "{{ name }}" grant all privileges on tables to "{{ name }}"; -- includes views ; must be called after new ones are created ?!
{% endset %}
{#% do log("create_role_if_not_exists sql " ~ sql, info=True) %#}

{% do run_query(sql) %}
{% do log("DONE create_role_if_not_exists " ~ name, info=True) %}
{% endmacro %}


{# on-run-start is not called by run-operation so let's define UDFs here rather than in on-run-start create_udfs() : #}
{% macro create_role_schema_udfs() %}
{% set sql %}
create or replace function public.create_role_if_not_exists(r text)
  returns text
as $$
BEGIN
    execute format($f$create role %I$f$, r); -- inspired by https://stackoverflow.com/questions/43527476/create-role-programmatically-with-parameters
    return null;
EXCEPTION
    WHEN duplicate_object THEN return NULL; -- RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    -- inspired by https://stackoverflow.com/questions/8092086/create-postgresql-role-user-if-it-doesnt-exist
END;
$$ language plpgsql;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
