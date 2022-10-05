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

TODO ? --args '{ schemas: ["france-data-reseau", "appuiscommuns", "sdirve", "eaupotable", "eclairage_public"] }'
#}


{#
as said in import.py :
creates a user with said password if it doesn't already exist.
Grants him the <schema + schema_suffix> roles
Also does the rights / permissions magic (allow other users permissions on tables created by this user :
FOR EACH USER AND EACH SCHEMA / ROLE (so for each schema_suffix also) alter default privileges in schema schemagroup grant all privileges on tables to schemagroup)
Usage :
set -a ; PASSWORD=`openssl rand -base64 ${1:-16}` ; dbt run-operation create_user --args '{name: "dbt_admin_user", schemas_string: "appuiscommuns,eaupotable,sdirve,eclairage_public,france-data-reseau"}' --target prod_(pg)admin(_stellio) ; set +a ; echo password : $PASSWORD

Gotchas :
- do not do : ALTER ROLE <user> SET ROLE = <schemagroup>; because that would prevent him from using his other roles
(schemagroup_test, mutualized group...), which would appear in the "rolconfig" column of pg_catalog.pg_roles ;
in which case undo it with : ALTER ROLE <user> SET ROLE = DEFAULT;
see https://www.postgresql.org/docs/current/sql-alterrole.html https://dba.stackexchange.com/questions/215549/automatically-invoke-set-role-when-connecting-to-postgresql
#}
{% macro create_user(name, schemas_string, password=env_var('PASSWORD'), schema_suffixes = var("schema_suffixes")) %}
{% set schemas = schemas_string.split(',') %}
{% set sql %}

-- schemas={{ schemas }} schema_suffixes={{ schema_suffixes }}
--CREATE USER "{{ name }}" IN GROUP {{ '"' + '","'.join(schemas) +'"' }} PASSWORD '{{ password }}' CREATEDB;
--CREATE USER "{{ name }}" PASSWORD '{{ password }}' CREATEDB;
select public.create_user_if_not_exists('{{ name }}', '{{ password }}');
{% for schema in schemas %}
    {% for schema_suffix in schema_suffixes %}
        grant "{{ schema ~ schema_suffix }}" to "{{ name }}";
    {% endfor %}
{% endfor %}

SET ROLE "{{ name }}";
{% for schema in schemas %}
    {% for schema_suffix in schema_suffixes %}
        alter default privileges in schema "{{ schema ~ schema_suffix }}" grant all privileges on tables to "{{ schema ~ schema_suffix }}";
        alter default privileges in schema "{{ schema ~ schema_suffix }}" grant all privileges on sequences to "{{ schema ~ schema_suffix }}"; -- for now not needed
        alter default privileges in schema "{{ schema ~ schema_suffix }}" grant all privileges on functions to "{{ schema ~ schema_suffix }}"; -- else another user's on-run-start create_udfs() will explode
    {% endfor %}
{% endfor %}
RESET ROLE;

{% endset %}
{% do log("create_user sql " ~ sql, info=True) %}
{% do run_query(sql) %}
{% do log("create_user " ~ name ~ " in " ~ schemas, info=True) %}
{% endmacro %}


{#
as said in import.py :
datalake structure init (or update) - create use case roles and (shared, not personal) schemas with rights.
must be run as DB admin (or a user having permission to create role and schema). NB. First runs create_udfs because it requires it.
Also adds required postgres extension uuid-ossp
Usage :
dbt run-operation create_roles_schemas --target prod_(pg)admin(_stellio)
#}
{% macro create_roles_schemas(schemas = var("schemas"), schema_suffixes = var("schema_suffixes"), shared_data_roles=["france-data-reseau"], dbt_admin_role="dbt_admin") %}

{# on-run-start is not called by run-operation so let's define UDFs here rather than in on-run-start create_udfs() : #}
{{ create_role_schema_udfs() }}

{% set sql %}
CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; -- TODO else dbt error : function uuid_ns_dns() does not exist
{% endset %}
{% do run_query(sql) %}
{#% do log("create_roles_schemas sql " ~ sql, info=True) %#}

{% for schema in schemas %}
    {% for schema_suffix in schema_suffixes %}
        {{ create_role_schema(schema ~ schema_suffix, shared_data_roles) }}
    {% endfor %}
{% endfor %}
{% endmacro %}


{% macro create_role_schema(name, shared_data_roles=["france-data-reseau"]) %}
--CREATE ROLE "datactivist";
--CREATE SCHEMA AUTHORIZATION "datactivist";
--alter default privileges in schema "datactivist" grant all privileges on tables to "datactivist";

{% set sql %}
select public.create_role_if_not_exists('{{ name }}');
CREATE SCHEMA IF NOT EXISTS AUTHORIZATION "{{ name }}";
alter default privileges in schema "{{ name }}" grant all privileges on tables to "{{ name }}"; -- NOO ; includes views ; must be called after new ones are created ?!

{# actually not required ? #}
{% for shared_data_role in shared_data_roles | select("ne", name) %}
GRANT "{{ shared_data_role }}" TO "{{ name }}";
{% endfor %}
{% endset %}
{#% do log("create_role_if_not_exists sql " ~ sql, info=True) %#}

{% do run_query(sql) %}
{% do log("DONE create_role_if_not_exists " ~ name, info=True) %}
{% endmacro %}


{#
on-run-start is not called by run-operation so let's define UDFs here rather than in on-run-start create_udfs() :
#}
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

create or replace function public.create_user_if_not_exists(u text, p text)
  returns text
as $$
BEGIN
    execute format($f$CREATE USER %I PASSWORD %L CREATEDB;$f$, u, p); -- inspired by https://stackoverflow.com/questions/43527476/create-role-programmatically-with-parameters
    return null;
EXCEPTION
    WHEN duplicate_object THEN return NULL; -- RAISE NOTICE '%, skipping', SQLERRM USING ERRCODE = SQLSTATE;
    -- inspired by https://stackoverflow.com/questions/8092086/create-postgresql-role-user-if-it-doesnt-exist
END;
$$ language plpgsql;
{% endset %}
{% do run_query(sql) %}
{% endmacro %}
