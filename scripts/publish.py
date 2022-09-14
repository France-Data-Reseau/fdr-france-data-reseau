'''
Publie une table SQL / model DBT au format CSV dans un jeu de données dédié dans CKAN
- script python fal à configurer dans un schema.yml sur une table SQL / modèle DBT à publier, avec au moins en "meta" FDR_ROLE
- création d'un jeu de données dédié s'il n'existe pas déjà,  avec métadonnées selon les champs personnalisés configurés dans le projet DBT et en "meta" d'un schema.yml
- conversion de la table SQL / modèle DBT vers CSV à la volée en SQL (macro DBT / SQL générique exécutée par fal). Ceci est plus puissant, plus générique et plus performant qu'une conversion en python pandas (ou la seule conversion de types faite actuelllement sert à éviter trop de mauvaise conversion par pandas ; bien que ce serait possible en introspectant les types dans la base SQL).
- TODO LATER : configuration du script plutôt une seule fois globalement (mais chaque model devrait quand même configurer au moins FDR_ROLE) ?

dans fdr-eaupotable :
# define env vars AND export them in subprocesses : https://unix.stackexchange.com/questions/79064/how-to-export-variables-from-a-file
set -a
source dbt_packages/fdr_appuiscommuns/env.prod
set +a
cp -rf ../fdr_appuiscommuns/scripts .
fal run --select eaupot

FAQ - CKAN :
- si erreur ckanapi.errors.ValidationError: {'name': ['Cette URL est déjà utilisée.
=> purger (supprimer définitivement) les jeux de données effacés, par exemple dans les outils de l'administrateur
le bouton Purge All dans l'onglet Poubelle : https://ckan.francedatareseau.fr/fr/ckan-admin/trash

TODO :
- externalize conf & vars, function-ize voire abstraire fal
- Erreur : The data was invalid: invalid input syntax for type numeric: "2018-06-13T00:00:00"
- remove fieldPrefix from published CSV
LATER :
- update only if changed since (ckan hash, dbt snapshot ??)
'''

from ckanapi import RemoteCKAN, ValidationError, NotAuthorized
import tempfile
from os import remove, getenv
from datetime import datetime

# for json output
import json


fdr_roles = {
    'echange' : {
        'title' : 'Données normalisées',
        'description' : "Version normalisée, unifiée, dédupliquée des données sources du cas d'usage",
        'package_tags' : ['normalisation'] # exemples ?, normalisation, indicateurs
    },
    'kpi' : {
        'title' : 'Indicateurs',
        'description' : "Indicateurs du cas d'usage",
        'package_tags' : ['indicateurs'] # exemples ?, normalisation, indicateurs
    }

}

targets = {
    'prod' : None,
    'test' : "DONNEES D'EXEMPLE",
    'staging' : "DONNEES DE DEVELOPPEMENT"
}

ua = 'ckanapifdr/1.0 (+https://ckan.francedatareseau.fr)'
fdrckan_url = 'https://ckan.francedatareseau.fr'
#fdrckan_url = 'http://172.18.0.14:5000'
fdrckan_apikey = getenv('FDR_SYNC_CKAN_API_KEY')

fdrckan = RemoteCKAN(fdrckan_url, apikey=fdrckan_apikey, user_agent=ua)

import yaml
with open('dbt_project.yml', 'r') as f:
    dbt_project_conf = yaml.safe_load(f)
    fdr_project = dbt_project_conf['name'].replace('fdr_', '') # appuiscommuns
    FDR_CAS_USAGE = dbt_project_conf['vars'].get('FDR_CAS_USAGE') # apcom ; None means not an officiela cas d'usage
    #base_schema = dbt_project_conf['vars'].get('base_schema') or fdr_project
    owner_org = context.current_model.meta.get("publish_org") or dbt_project_conf['vars'].get('owner_org') or 'usage-' + fdr_project.replace('_', '-') # usage-appuis-communs
    use_case_prefix = dbt_project_conf['vars'].get('use_case_prefix') or FDR_CAS_USAGE or fdr_project # apcom

target_name = execute_sql("select '{{ target.name }}'").values[0][0]
schema = execute_sql("select '{{ target.schema }}'").values[0][0]
base_schema = schema.replace('_' + target_name, '') if target_name else schema
#FDR_TARGET = 'prod' # TODO test prod
# ensuring FDR_TARGET is among handled ones :
FDR_TARGET = target_name if target_name in targets else 'test'

FDR_ROLE = context.current_model.meta.get("FDR_ROLE") # 'source' 'echange' 'kpi' 'perimetre'
target_suffix = ('_' + FDR_TARGET) if FDR_TARGET and FDR_TARGET != 'prod' else ''
#schema = base_schema + target_suffix

print('publish conf', fdrckan_url, FDR_CAS_USAGE, owner_org, use_case_prefix, schema, FDR_TARGET, FDR_ROLE)

'''
Gets or creates package using configured metadata
'''
def publish_role_package(FDR_ROLE, FDR_TARGET):
    if not FDR_ROLE:
        exit()

    if FDR_ROLE in fdr_roles:
        fdr_role = fdr_roles[FDR_ROLE]
    else:
        fdr_role = {
                       'title' : FDR_ROLE,
                       'description' : FDR_ROLE,
                       'package_tags' : ["plateforme"]
                   }

    name = schema + '-' + FDR_ROLE
    title = (targets.get(FDR_TARGET) + ' - ' if targets.get(FDR_TARGET) else '') + fdr_role['title']
    #description = "MIS EN LIGNE AUTOMATIQUEMENT - jeux d'exemple des transformations SQL DBT du cas d'usage"
    description = "MIS EN LIGNE AUTOMATIQUEMENT - " + (targets.get(FDR_TARGET) + ' - ' if targets.get(FDR_TARGET) else '') + fdr_role['description']
    private = False if FDR_ROLE == 'kpi' or FDR_TARGET == 'exemple' else True # DON'T open up source or echange
    extras = [
        { 'key' : 'FDR_CAS_USAGE', 'value' : FDR_CAS_USAGE },
        { 'key' : 'FDR_ROLE', 'value' : FDR_ROLE }, # 'test' 'source' 'echange' 'kpi' 'perimetre'
        #[ 'key' : 'FDR_SOURCE_NOM', 'value' : FDR_SOURCE_NOM }, 'apcom_osm' 'apcom_aat_gthdv2' (megalis) 'apcom_equip_birdz'
    ]
    license_id = 'ODbL-1.0'
    tags = [{ 'name' : 'exemples' }] # normalisation, indicators
    # author, maintainer
    return get_or_create_package(owner_org=owner_org, name=name, title=title, description=description,
                                 private=private, extras=extras, license_id=license_id, tags=tags)


'''
Gets or creates package using given metadata
'''
def get_or_create_package(owner_org, name, title, description, private, extras, license_id, tags):
    # get or create target dataset :
    # TODO udpate if changed since
    #packages = demo.action.package_search(q='+organization:sample-organization +res_format:GeoJSON +tags:geojson')
    found_packages = fdrckan.action.package_search(q='+organization:"' + owner_org + '" +name:"' + name + '"', include_private=True)
    print('publish to package', owner_org, name, 'existing ? :', found_packages)
    if found_packages['results'] and len(found_packages['results']) != 0:
        pkg = found_packages['results'][0]
    else:
        try:
            # or package_show(id (string) – the id or name of the dataset
            pkg = fdrckan.action.package_create(owner_org=owner_org, name=name, title=title, description=description,
                                                private=private, extras=extras, license_id=license_id, tags=tags)
        except (NotAuthorized, ValidationError) as e:
            # ValidationError if name already exists
            # TODO may happen if Solr has not updated yet ??
            print('publish get_or_create_package denied', owner_org, name)
            raise e
    return pkg


'''
Calls publish_datastore_resource() if not found or should update (TODO)
'''
def publish_resource(pkg):
    # upload :
    # TODO udpate if changed since ?
    # hash ckan hash is mainly for internal use by datapusher etc. https://docs.getdbt.com/reference/dbt-jinja-functions https://github.com/ckan/datapusher/blob/1538e496e5181f94a873f233651c9021f2e676e8/datapusher/jobs.py#L370-L401
    # so for now upload always
    # TODO LATER rather try dbt snapshots ?
    #resource_name = 'apcom_osm_supportaerien_extract.csv'
    resource_name = context.current_model.name + '.csv'
    # NOT USING resource_search because doesn't support package_id
    # found_resources = fdrckan.action.resource_search(query='+package_id:"' + pkg['id'] + '" +name:"' + resource_name + '"')
    #if found_resources['results'] and len(found_resources['results']) != 0:
    #    res = found_resources['results'][0]
    found_resources = list(filter(lambda r: r['name'] == resource_name, pkg['resources'])) if pkg['resources'] else None
    found_resource = found_resources[0] if found_resources and len(found_resources) != 0 else None
    print('publish resource', resource_name, 'existing ? :', found_resource)
    should_update = True
    if not found_resource or should_update:
        publish_datastore_resource(pkg, resource_name, found_resource)


# NOT USED
def publish_ckan_resource(pkg, resource_name, found_resource):
    model_df = ref(context.current_model.name + '_csv')
    print('publish meta', context.current_model.meta)

    path = context.current_model.name + '.csv'
    #fd, path = tempfile.mkstemp() # NO otherwise CKAN resource has unreadable file name
    try:
        with open(path, 'w+b') as tmp:
           print('publish file', tmp.name, path)
           model_df.to_csv(tmp)
           tmp.close() # flush() and reuse tmp is not enough ?!
        #on error explode

        with open(path, 'r') as tmp:
            #print('publish file', tmp.read())
            try:
                ckan_resource_create_or_update = fdrckan.action.resource_create if not found_resource else fdrckan.action.resource_update
                res = ckan_resource_create_or_update(
                   id=found_resource['id'] if found_resource else None,
                   package_id=pkg['id'],
                   name=resource_name,
                   format='CSV',
                   mimetype="text/csv",
                   description="MIS EN LIGNE AUTOMATIQUEMENT", # TODO dbt metas ?!! from target/ ??
                   upload=tmp)
            except NotAuthorized as e:
                print('publish denied')
                raise e
    finally:
        remove(path)


'''
Used by publish_datastore_resource
'''
def build_resource(pkg, resource_name, found_resource, fields, records):
    data = {
        'resource_id' : found_resource['id'] if found_resource else None,
        'resource': {
            'resource_id' : found_resource['id'] if found_resource else None,
            'package_id': pkg['id'],
            'name' : resource_name,
            'format' : 'CSV',
            'mimetype' : "text/csv", # not filled by CKAN in db (?)
            ##'size' : "text/csv", # TODO not filled by CKAN in db (?) BUT not easy to compute without outputting to CSV first
            'last_modified' : datetime.utcnow().isoformat(), # TODO not filled by CKAN in db (?)
            'description' : "MIS EN LIGNE AUTOMATIQUEMENT", # TODO dbt metas ?!! from target/ ??
        } if not found_resource else None,
        'fields': fields,
        'records': records,
        #'primary_key': ['code'],
        'force': True
    }
    return data


def field_name_sql_to_csv(name):
    return name.replace(use_case_prefix + '_', '')

'''
Implementation of publish_resource()
TODO in batches if possible to avoid 504 Gateway timeout ?
'''
def publish_datastore_resource(pkg, resource_name, found_resource):
    # conversion :
    # NB. conversion to text for csv is done in SQL rather than in python, because dbt conversion to python has limits
    # ex. in case of dates : ValueError: year 20222 is out of range if year > 10000 and minimal year is 1970
    # because uses fromtimestamp https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp
    #model_df = execute_sql('select "eaupotcan_datePose"::date from {{ ref("' + context.current_model.name + '") }} where "eaupotcan_datePose"::text LIKE \'20222%\' limit 1')
    # ValueError: year 20222 is out of range dbt/adapters/sql/connections.py", line 115, in get_result_from_cursor https://docs.python.org/3/library/datetime.html#datetime.datetime.fromtimestamp
    # BUT maybe providing types from SQL to datastore works if it is pure PostgreSQL (COPY...) and not python insert (see further)
    ##model_df = ref(context.current_model.name) #  + '_csv'
    #model_df = execute_sql('select * from {{ ref("' + context.current_model.name + '") }} limit 1')
    # executing conversion macro on the fly :
    model_df = execute_sql('{{ fdr_appuiscommuns.to_csv(ref("' + context.current_model.name + '"), wkt_rather_than_geojson=true, prefix_regex_to_remove="' + use_case_prefix + '.*_") }}')
    print('publish model_df', model_df.head(5))

    print('publish meta', context.current_model.meta)
    #print('publish context', context)

    test_fields = [
        {'id': 'f', 'type': 'float'},
        {'id': 'place', 'type': 'text'},
    ]
    test_data = {
        'resource': {
            # 'id' ?!?
            'package_id': pkg['id'],
            'name': 'Test data',
            'format': 'csv',
        },
        'records': [{'f':1.1, 'place':'tt'},{'f':1.1, 'place':'ttt'}],
        'fields': test_fields,
        #'primary_key': ['code'],
    }

    # (datastore types are postgresql's)
    # TODO or using fal's way using pandas dtypes ?
    pandas_type_to_datastore = {
        'text' : 'text',
        'object' : 'text',
        'int64' : 'numeric',
        'float64' : 'numeric',
        'datetime64' : 'timestamp',
        'bool' : 'bool'
    }

    records = model_df.to_dict(orient='records')
    print('publish records - first :', records[0] if len(records) != 0 else None)

    #print('publish dtypes', model_df.dtypes.to_dict())
    #mydtype = model_df.dtypes.to_dict()['apcomindoc_all_count']
    #print('publish mydtype', str(mydtype))
    fields = [{'id' : field_name_sql_to_csv(name), 'type': pandas_type_to_datastore[str(t)]} for name, t in model_df.dtypes.to_dict().items()]
    # TODO LATER better (pg) fields for datastore using PostgreSQL :
    def_df = execute_sql('select * from information_schema.columns limit 1')
    print('publish def_df', def_df.to_dict(orient='records')) # def_df.dtypes,

    print('publish fields', fields)
    data = build_resource(pkg, resource_name, found_resource, fields, records)

    try :
        res = fdrckan.action.datastore_create(**data)

    except ValidationError as e:
        # This endpoint can be called multiple times to initially insert more data, add fields, change the aliases or indexes as well as the primary keys.
        # BUT it can't remove fields (though it can retype them)
        # => either create in a new dataset but then there will be many, or only if fails such,
        # or always delete first but then will lose defined ckan views, or flag them for their owner to delete them but then he will also lose the views to help him recreating them
        print('publish ValidationError, create another, timestamped resource')
        data = build_resource(pkg, datetime.now().isoformat() + '_' + resource_name, None, fields, records)
        res = fdrckan.action.datastore_create(**data)


'''
Gets or creates package then creates or udpates (datastore) resource in it
'''
def publish():
    pkg = publish_role_package(FDR_ROLE, FDR_TARGET)
    publish_resource(pkg)

publish()