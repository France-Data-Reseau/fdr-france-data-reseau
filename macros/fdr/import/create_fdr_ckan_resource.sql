{#
To be run at install
#}
{% macro create_fdr_ckan_resource() %}
{% if execute %}
{% do log("create_fdr_ckan_resource_nifi", info=False) %}
{% set sql %}
BEGIN;
CREATE TABLE if not exists "{{ target.schema }}".fdr_ckan_resource (
	id text NULL,
	"name" text NULL,
	created timestamp NULL,
	last_modified timestamp NULL,
	"size" int8 NULL,
	format text NULL,
	extras text NULL,
	url text NULL,
	ds_id text NULL,
	ds_name varchar(100) NULL,
	ds_title text NULL,
	ds_metadata_created timestamp NULL,
	ds_metadata_modified timestamp NULL,
	package_id text NULL,
	fdr_cas_usage text NULL,
	fdr_role text NULL,
	fdr_source_nom text NULL,
	fdr_target text NULL,
	org_id text NULL,
	org_name text NULL,
	org_title text NULL,
	group_id text NULL,
	fdr_siren text NULL,
	u_id text NULL,
	u_email text NULL,
	u_name text NULL,
	last_changed timestamp NULL
);
-- index ONLY on id, and not on (id, dsid, orgid) else duplicate lines ex. if dataset is moved in another org !
-- (and this index' columns must be in Nifi PutDatabaseRecord' Update keys ;
-- which does not support underscore in column names ex. dsid but not ds_id :
-- Nifi removes _ from update keys https://issues.apache.org/jira/browse/NIFI-5608 )
CREATE UNIQUE INDEX fdr_ckan_resource_idx ON "france-data-reseau".fdr_ckan_resource USING btree (id);
-- else error there is no unique or exclusion constraint matching the on conflict specification :
-- https://stackoverflow.com/questions/42022362/no-unique-or-exclusion-constraint-matching-the-on-conflict
COMMIT; -- else does not create ! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
{% endset %}
{% do log("create_fdr_ckan_resource SQL " ~ sql, info=False) %}
{% do run_query(sql) %}
{% do log("create_fdr_ckan_resource source_row done", info=True) %}
{% endif %}
{% endmacro %}