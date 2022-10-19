
-- view over CKAN data required by import.py :
-- (to be synchronized to DBT datastore ex. using Nifi or DBeaver-CE)
-- Nifi conf & gotchas :
-- - put it in QueryDatabaseTable Query parameter
-- - orgid and dsid without _ else when used in Nifi PutDatabaseRecord's Update Keys (incremental), error (PK for ON CONFLICT)
-- - using a single Maximum-value column for Nifi QueryDatabaseTable,
-- else if 2 columns are provided to it, they should BOTH increase to trigger its processing
-- https://community.cloudera.com/t5/Support-Questions/QueryDatabaseTable-with-an-OR-instead-of-AND-when-using/m-p/308780
-- NB. a consequence is that if we provide both of them, it doesn't work if r.last_modified is NULL i.e. external resource case
create or replace view fdr_resource as (

with dsex as (
select
    package_id,
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_CAS_USAGE'))[1]) as "fdr_cas_usage", -- "FDR_CAS_USAGE"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_ROLE'))[1]) as "fdr_role", -- "FDR_ROLE"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_SOURCE_NOM'))[1]) as "fdr_source_nom", -- "FDR_SOURCE_NOM"
    trim((ARRAY_AGG(dsex.value) FILTER (WHERE trim(dsex.key) = 'FDR_TARGET'))[1]) as "fdr_target" -- "FDR_TARGET"
from package_extra dsex
group by dsex.package_id
),
orgex as (
select
    group_id,
    (ARRAY_AGG(orgex.value) FILTER (WHERE orgex.key = 'FDR_SIREN'))[1] as "fdr_siren" -- "FDR_SIREN"
from group_extra orgex
group by orgex.group_id
), r_enriched as (
select
r.id, r.name, r.created, r.last_modified, r.size, r.format, r.extras, r.url,
ds.id as dsid, ds.name as ds_name, ds.title as ds_title,
ds.metadata_created  as ds_metadata_created, ds.metadata_modified  as ds_metadata_modified,
dsex.*,
org.id as orgid, org.name as org_name, org.title as org_title, -- label
orgex.*,
u.id as u_id, u.email as u_email, u.name as u_name
from resource r -- NB. resource has no _extra table but resource.extras can contain json !
join package ds on r.package_id=ds.id
join dsex on ds.id=dsex.package_id
join public.group org on ds.owner_org=org.id
join orgex on org.id=orgex.group_id
join public.user u on ds.creator_user_id=u.id
where
org.is_organization -- else mere group
and "fdr_cas_usage" is not null -- "FDR_CAS_USAGE"
--and r.format in ('CSV', 'Excel')
and r.state = 'active' and ds.state = 'active'
)
select *,
-- building Maximum-value column for Nifi QueryDatabaseTable :
-- else if 2 columns are provided to it, they should BOTH increase to trigger its processing
-- https://community.cloudera.com/t5/Support-Questions/QueryDatabaseTable-with-an-OR-instead-of-AND-when-using/m-p/308780
-- NB. a consequence is that if we provide both of them, it doesn't work if r.last_modified is NULL i.e. external resource case
GREATEST((CASE WHEN r_enriched.last_modified IS NULL then r_enriched.ds_metadata_modified else r_enriched.last_modified end), r_enriched.ds_metadata_modified) as last_changed
from r_enriched

)