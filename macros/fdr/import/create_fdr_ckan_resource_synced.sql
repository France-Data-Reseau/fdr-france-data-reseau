{% macro create_fdr_ckan_resource_synced() %}
{% if execute %}
{% do log("create_fdr_ckan_resource_synced", info=False) %}
{% set sql %}
BEGIN;
CREATE TABLE if not exists "{{ target.schema }}".fdr_ckan_resource_synced (
	id text NULL,
	"name" text NULL,
	last_modified timestamp NULL,
	"size" int8 NULL,
	format text NULL,
	extras text NULL,
	url text NULL,
	dsid text NULL, -- Nifi removes _ from update keys https://issues.apache.org/jira/browse/NIFI-5608
	ds_name varchar(100) NULL,
	ds_title text NULL,
	ds_metadata_modified timestamp NULL,
	package_id text NULL,
	fdr_cas_usage text NULL,
	fdr_role text NULL,
	fdr_source_nom text NULL,
	fdr_target text NULL,
	orgid text NULL, -- Nifi removes _ from update keys https://issues.apache.org/jira/browse/NIFI-5608
	org_name text NULL,
	org_title text NULL,
	group_id text NULL,
	fdr_siren text NULL,
	u_id text NULL,
	u_email text NULL,
	u_name text NULL
);
CREATE UNIQUE INDEX if not exists fdr_ckan_resource_synced_idx on "{{ target.schema }}".fdr_ckan_resource_synced (id, dsid, orgid);
COMMIT; -- else does not create ! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
{% endset %}
{% do log("create_fdr_ckan_resource_synced SQL " ~ sql, info=False) %}
{% do run_query(sql) %}
{% do log("create_fdr_ckan_resource_synced source_row done", info=True) %}
{% endif %}
{% endmacro %}