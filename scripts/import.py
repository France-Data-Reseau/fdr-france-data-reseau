'''
Import (incrémental) des données des ressources CKAN selon les champs personnalisés FDR_ dans la base SQL datalake
- se base sur la table public.fdr_resource de toutes les ressources CKAN ayant des champs personnalisés FDR configurés sur leur jeu de données. Pour l'instant synchronisée à la main dans la base datastore depuis la vue SQL définie dans la base CKAN, TODO LATER automatiquement par l'ELT Airbyte. Cette méthode est plus efficace que de nombreux appels à l'API CKAN.
- pour chacune :
  - vérification de ses champs personnalisés FDR,
  - et de la nécessité d'importer (pas si archive, que si changement depuis le dernier import réussi),
  - téléchargement depuis CKAN par son API
  - puis import selon le format.
- Actuellement seul geopackage est supporté (par exécution de la commande ogr2ogr de GDAL, pouvant être dockérisée)
  - mais geojson et autres formats géographiques peuvent l'être facilement par la même méthode.
  - CSV pourrait l'être aussi, par pandas avec un peu plus de travail, à moins de rester sur la visibilisation des tables importées par CKAN dans le datastore (pour l'instant manuelle cependant).
- résultats et erreurs écrits dans la table france-data-reseau.fdr_import_resource
  - qui sert de référence à tous les traitements exploitant les données importées pour en retrouver les tables et leur configuration (notamment du type de traitement à appliquer, par FDR_SOURCE_NOM), qu'ils soient en DBT / SQL ou en python.
  - mis à disposition auprès des collectivités en CSV dans CKAN  dans l'organisation FDR par le script publish.py
- TODO : support du format CSV, voire geojson (et bien sûr automatisation de l'ensemble des opérations) ; exécution plutôt une fois par projet DBT cas d'usage ? ou au contraire plus simplement même de leurs traitements en une seule fois dans un projet DBT qui dépend de tous ?

OBSOLETE introspect CKAN API to :
- build list of datastore-imported files (ex. FDR_ROLE=source, format=gepackage) to make visible as views using dbt on-run-start macro
- download other files that have to be imported, and import them :
  - geojson (FDR_ROLE=perimetre), using COPY ?? NO have to add the organization_name OR RATHER org.SIREN (from org extra fields / custom keys) !
  - geopackage (FDR_ROLE=source, format=geopackage), using dockerized ogr2ogr (or in Airflow ??) (TODO only if newer udpate date)
- import only those that have changed (according to the local copy of fdr_resource), unless deleting the import state file 'import_state.txt'
example :

rajouter dans des jeux de données de cas d'usage
  - avec les champs personnalisés requis :
    - exemple : FDR_CAS_USAGE=eaupotable, FDR_ROLE=source, FDR_SOURCE_NOM=canalisations_en_service, FDR_TARGET=prod
  - et dedans créer des ressources en mettant en ligne des fichiers de données
    - ayant les champs impliqués par le modèle de données correspondant défini dans le JDB

cd fdr-france-data-reseau/

# platform init (or update) - create use case roles and (shared, not personal) schemas with rights :
# (first runs create_udfs because it requires it)
dbt run-operation create_roles_schemas --target prod_sync

# import CKAN resource files in DB and create meta / report table :
# params have to be provided in environment variables (see env.template & README ; set -a propagates them to subshells / commands) :
set -a ; source env.prod ; set +a
# IF NOT CREATED by previous command, create current (prod_sync) schema for fal import or on-run-start create_udfs (at least one model is required) :
dbt run --select meta --target prod_sync
fal run --before
# or
fal run --target prod_sync --before

#create views UNIONing imported tables of the same FDR_SOURCE_NOM :
NON dbt run-operation create_views_union --target prod_sync, plutôt DANS un projet cas d'usage, par exemple :
cd fdr-eaupotable/
dbt run --select eaupot_src_canalisations_parsed

or in test :
set -a ; source env.test ; set +a
cd fdr-france-data-reseau/
# IF NOT CREATE by previous command, create current (prod_sync) schema for fal import or on-run-start create_udfs (at least one model is required) :
dbt run --select meta --target prod_sync
fal run --target prod_sync_test --before
cd fdr-eaupotable/
dbt run --target test --select eaupot_src_canalisations_parsed
'''



import os
from ckanapi import RemoteCKAN, NotAuthorized

ua = 'ckanapifdr/1.0 (+https://ckan.francedatareseau.fr)'
fdrckan_url = os.getenv("FDR_SYNC_CKAN_URL")
#fdrckan_url = 'https://ckan.francedatareseau.fr'
#fdrckan_url = 'http://172.18.0.14:5000'
fdrckan_apikey = os.getenv("FDR_SYNC_CKAN_API_KEY")
fdrckan = RemoteCKAN(fdrckan_url, apikey=fdrckan_apikey, user_agent=ua)

ogr2ogr_command = os.getenv("FDR_SYNC_OGR2OGR_COMMAND")
host = os.getenv("FDR_SYNC_POSTGRES_HOST")
port = os.getenv("FDR_SYNC_POSTGRES_PORT")
database = os.getenv("FDR_SYNC_POSTGRES_DATABASE")
private_test_schema = os.getenv("FDR_SYNC_POSTGRES_PRIVATE_TEST_SCHEMA") # TODO or even _timestamp tables then published as views !!!
user = os.getenv("FDR_SYNC_POSTGRES_USER")
password = os.getenv("FDR_SYNC_POSTGRES_PASSWORD")

# TODO as params
resource_status_source = 'france-data-reseau'
#resource_status_table = 'fdr_import_resource'
resource_status_table = 'fdr_import_resource_dev'

#FDR_CAS_USAGE = 'apcom'
#FDR_CAS_USAGE = 'eaupotable'
#schema = 'appuiscommuns_test'
#use_case_prefix = 'eaupot'
# TODO better elsewhere / deduce :
fdr_cas_usages = {
    'apcom' : {
        'use_case_prefix' : 'apcom',
        'schema' : 'appuiscommuns'
    },
    'sdirve' : {
        'use_case_prefix' : 'sdirve',
        'schema' : 'sdirve'
    },
    'eaupotable' : {
        'use_case_prefix' : 'eaupot',
        'schema' : 'eaupotable'
    },
    'eclairage_public' : {
        'use_case_prefix' : 'eclpub',
        'schema' : 'eclairage_public'
    },
    'france-data-reseau' : {
        'use_case_prefix' : 'fdr',
        'schema' : 'france-data-reseau'
    }
}

#id_aode = 'sireneaudazur'
#FDR_SOURCE_NOM = 'canalisation' # TODO native canalisation
#step = 'imported'
#source_file = "/home/mdutoo/dev/ozwillo/fdr/data/EAU D'AZUR/2022-03-09_802630608_canalisations_en_service.gpkg"
#appuiscommuns_test.eaupot_src_native_sireneaudazur_canalisation
#source_file = '.geojson'

formats = {
    'gpkg' : ['gpkg', 'geopackage'],
    'geojson' : ['geojson'],
    'csv' : ['csv']
}
all_formats = [ext for f in formats for ext in formats[f]]
#print(all_formats)

# data completing hacks :canalisations_abandonees
fdr_source_noms = ['canalisations_en_service', 'canalisations_abandonnees', 'reparations',
                   'perimetre', #'pointlumineux', 'dictionnaire_champs_valeurs'
                   ]

# NOT USED
def get_from_ckan():
  found_packages = fdrckan.action.package_search(q='+FDR_CAS_USAGE:"' + FDR_CAS_USAGE + '"', include_private=True)
  print(found_packages)


import json
import os, subprocess
import requests
import re
from pathlib import Path
from datetime import datetime
import pandas


import_state_file = 'import_state.txt'
try:
    with open(import_state_file, "r") as f:
        import_state = json.load(f)
except FileNotFoundError as e: # if JSONDecodeError, rather remove file
    print('import : ', import_state_file, 'not found, using empty state and importing all resources')
    import_state = {}

def compute_has_changed(resource, step='default'):
    # TODO datetime.utcfromtimestamp(timestamp1)
    last_changed_string = build_last_changed_string(resource)
    resource_key = build_resource_key(resource, step)
    has_stayed_same = resource_key in import_state and last_changed_string and import_state[resource_key] == last_changed_string
    if has_stayed_same:
        print('import not changed, skip', step, resource, resource_key, import_state.get(resource_key), last_changed_string)
    else:
        print('import changed', resource, resource_key, import_state.get(resource_key), last_changed_string)
    return not has_stayed_same

def set_changed(resource, step='default'):
    last_changed_string = build_last_changed_string(resource)
    resource_key = build_resource_key(resource, step)
    import_state[resource_key] = last_changed_string
    with open(import_state_file, 'w') as f:
        json.dump(import_state, f, indent=4)
    print('import set_changed', resource_key, last_changed_string)

def build_resource_key(resource, step='default'):
    resource_key = '/'.join([resource['org_name'], resource['ds_name'], resource['name'], resource['id'], step])
    return resource_key

def build_last_changed_string(resource):
    last_modified = resource.get('last_modified')
    ds_metadata_modified = resource.get('ds_metadata_modified')
    last_changed = last_modified if not ds_metadata_modified else ds_metadata_modified if not last_modified else last_modified if last_modified > ds_metadata_modified else ds_metadata_modified
    last_changed_string = last_changed.isoformat() if last_changed else None
    return last_changed_string


def download_ckan_resource(resource):
    step = 'download'
    source_file_path = 'ckan/' + resource['org_name'] + '/' + resource['name']
    if not compute_has_changed(resource, step):
        # skip download
        return source_file_path

    Path(source_file_path).parents[0].mkdir(parents=True, exist_ok=True)
    url = '/'.join([fdrckan_url, 'dataset', resource['ds_id'], 'resource', resource['id'], 'download', resource['url']])
    print('import download_ckan_resource', url, source_file_path, resource, import_state.get(url))
    headers = {
        "Authorization": fdrckan_apikey
    }
    resp = requests.get(url, headers=headers)
    with open(source_file_path, "wb") as f:
        f.write(resp.content)

    set_changed(resource, step)
    return source_file_path


def ogr2ogr(source_file, schema_and_table):
    # see https://gis.stackexchange.com/questions/154004/execute-ogr2ogr-from-python
    print(ogr2ogr_command, host, "' port='", port, "' user='", user, "' password='", password, "' dbname='", database, os.path.abspath(source_file), schema_and_table)
    command = ogr2ogr_command.split(' ') + [
          #"--config", "PG_LIST_ALL_TABLES", "YES",
          "-f", "PostgreSQL",
          "-overwrite", # else not append, and if nothing command blocks when already exists ?!
          "PG:host='" + host + "' port='" + port + "' user='" + user + "' password='" + password + "' dbname='" + database + "'",
           os.path.abspath(source_file), # else FAILURE: Unable to open datasource...
           "-nln", schema_and_table] # don't quote else they go in the name ! but ogr2ogr replaces ex. - by _...
    #print(' '.join(command))
    try:
        subprocess.run(command, check=True, capture_output=True)
        # https://stackoverflow.com/questions/39563802/subprocess-calledprocesserror-what-is-the-error
        return None
    except subprocess.CalledProcessError as e:
        print(e)
        return e.stderr.decode("utf-8") # e.output ; if not decode, sqlalchemy typeError: a bytes-like object is required, not 'str'

def ogr2ogr_geojson(source_file, schema_and_table):
    # see https://gis.stackexchange.com/questions/154004/execute-ogr2ogr-from-python
    command = ogr2ogr_command.split(' ') + [
        #"--config", "PG_LIST_ALL_TABLES", "YES",
        "-f", "PostgreSQL",
        "-overwrite", # else not append, and if nothing command blocks when already exists ?!
        "PG:host='" + host + "' port='" + port + "' user='" + user + "' password='" + password + "' dbname='" + database + "'",
        source_file,
        "-nln", schema_and_table] # don't quote else they go in the name ! but ogr2ogr replaces ex. - by _...
    #print(' '.join(command))
    try:
        subprocess.run(command, check=True, capture_output=True)
        # https://stackoverflow.com/questions/39563802/subprocess-calledprocesserror-what-is-the-error
        return None
    except subprocess.CalledProcessError as e:
        print(e)
        return e.stderr.decode("utf-8") # e.output ; if not decode, sqlalchemy typeError: a bytes-like object is required, not 'str'


def import_resource(resource, import_state):
    state = {

    }
    # TODO LATER fillimport_state with import state & info such as :
    resource_start = datetime.now().isoformat()
    messages = []

    FDR_CAS_USAGE = resource['fdr_cas_usage'] # always there ; TODO 'FDR_CAS_USAGE'
    if FDR_CAS_USAGE not in fdr_cas_usages or len(FDR_CAS_USAGE.strip()) == 0:
        messages.append({ "status" : "error", "text" : "undefined FDR_CAS_USAGE" })
        schema, use_case_prefix = None, None
    else:
        schema = fdr_cas_usages[FDR_CAS_USAGE]['schema']
        ##schema = fdr_cas_usages[FDR_CAS_USAGE]['schema'] + (import_state['schema_suffix'] if import_state['schema_suffix'] else '')
        use_case_prefix = fdr_cas_usages[FDR_CAS_USAGE]['use_case_prefix']

    FDR_ROLE = resource['fdr_role'] if resource['fdr_role'] is not None and len(resource['fdr_role'].strip()) != 0 else 'source' # TODO 'FDR_ROLE'
    print('import_resource', resource['name'], FDR_ROLE)
    if FDR_ROLE != 'source':
        return

    data_owner_id_tmp = resource['fdr_siren'] if resource['fdr_siren'] is not None and len(resource['fdr_siren'].strip()) != 0 else resource['org_id'][:4] + resource['org_name'][-6:] # TODO FDR_SIREN
    # make it safe for postgres, else ogr2ogr does it (and doesn't allow quoting) :
    data_owner_id = re.sub('[^a-zA-Z0-9_]', '', data_owner_id_tmp.lower())
    format = (resource['format'] if resource['format'] is not None else os.path.splitext(resource['name'])).lower()
    print('import_resource', resource['name'], data_owner_id, format)

    if format not in all_formats:
        messages.append({ "status" : "error", "text" : "unsupported format" })

    if resource['fdr_source_nom'] is not None and len(resource['fdr_source_nom'].strip()) != 0: # TODO FDR_SOURCE_NOM
        FDR_SOURCE_NOM = resource['fdr_source_nom']
    else:
        found_sn = [sn for sn in fdr_source_noms if sn in resource['name']]
        #FDR_SOURCE_NOM, message = found_sn[0], None if len(found_sn) else None, "missing FDR_SOURCE_NOM"
        if len(found_sn) == 0:
            messages.append({ "status" : "error", "text" : "missing FDR_SOURCE_NOM" })
            FDR_SOURCE_NOM = None
            print('no FDR_SOURCE_NOM, aborting', resource['name'])
        else :
            FDR_SOURCE_NOM = found_sn[0]

    FDR_TARGET = resource['fdr_target'] if resource['fdr_target'] is not None and len(resource['fdr_target'].strip()) != 0 else 'prod' # ? ; TODO 'FDR_TARGET'
    # TODO FDR_TARGET != prod => test schema

    # skip download and below import if archive :
    # (or could put target in table name, but rather in schema for dbt, and anyway is faster)
    if FDR_TARGET != 'archive':

        try:
            source_file_path = download_ckan_resource(resource)
        except Exception as e:
            messages.append({ "status" : "error", "text" : "while downloading " + str(e) })
            source_file_path = None

    else :
        messages.append({ "status" : "skipped", "text" : "is archive so skipped import" })
        source_file_path = None

    if len([m for m in messages if m['status'] == 'error']) != 0 or FDR_TARGET == 'archive':
        step = "prepare"
        table = None

    else:

        #table = '_'.join([use_case_prefix, 'raw', FDR_SOURCE_NOM, data_owner_id, step])
        table = '_'.join([use_case_prefix, 'raw', FDR_SOURCE_NOM, data_owner_id])
        print('import_resource', resource['name'], FDR_SOURCE_NOM, data_owner_id, format, schema, table, source_file_path)

        schema_and_table = schema + '.' + table
        if schema_and_table in import_state['schema_and_tables']:
            messages.append({ "status" : "error", "text" : "schema and table duplicate " + schema_and_table })
            step = "prepare"

        elif compute_has_changed(resource, 'insert'):
            import_state['schema_and_tables'].append(schema_and_table)

            if format in formats['gpkg']:
                # TODO in org schema ! and afterwards create view with schema, use_case_prefix
                print('supported format', format, resource['name'])
                #return
                res = ogr2ogr(source_file_path, schema_and_table)
                step = "ogr2ogr"

            elif format in formats['geojson']: # TODO "perimetre"
                print('supported format', format, resource['name'])
                res = ogr2ogr_geojson(source_file_path, schema_and_table)
                step = "ogr2ogr"

            elif format in formats['csv']:
                print('supported format', format, resource['name'])
                # TODO create view or download & import ?
                res = "in development"
                step = "in development"

            else:
                # should not happen !
                # NB. unsupported formats already filtered out ???
                res = "unsupp"
                step = "unsupp"

            if res:
                messages.append({ "status" : "error", "text" : res })
                #print('ogr2ogr error', res, {i:eval(i) for i in ["schema", "use_case_prefix", FDR_SOURCE_NOM, data_owner_id, source_file_path]})
            else:
                set_changed(resource, 'insert')

        else:
            step = "no change, skipped"
            messages.append({ "status" : "success", "text" : "no change, already imported at " + build_last_changed_string(resource) })

    status = "error" if len([m for m in messages if m['status'] == 'error']) != 0 else "skipped" if len([m for m in messages if m['status'] == 'skipped']) != 0 else "success"

    return {
        "FDR_CAS_USAGE" : FDR_CAS_USAGE, "FDR_ROLE" : FDR_ROLE, "FDR_SOURCE_NOM" : FDR_SOURCE_NOM, "FDR_TARGET" : FDR_TARGET,
        "status" : status, "start" : resource_start, "end" : datetime.now().isoformat(),
        "component": "import.py", "step": step, # "subcomponent" or "step" ?
        "resource_id" : resource['id'], "resource_name" : resource['name'],
        "dataset_id" : resource['ds_id'], "dataset_name" : resource['ds_name'], "dataset_title" : resource['ds_title'],
        "org_id" : resource['org_id'], "org_name" : resource['org_name'], "org_title" : resource['org_title'], # label
        "data_owner_id" : data_owner_id, "format" : format, "source_file_path" : source_file_path,
        "schema" : schema, "table" : table, "use_case_prefix" : use_case_prefix,
        "messages" : json.dumps(messages) # , indent = 4
    }

def import_resources(schema_suffix = ''):
    print('import_resources start')
    print('import_resources params:', fdrckan_url, ogr2ogr_command, host, port, database, user)
    print('import_resources conf:', fdr_cas_usages, formats, fdr_source_noms)

    import_state = {
        "schema_suffix" : schema_suffix, ##
        "schema_and_tables" : [], # used to prevent duplicates
        "resource_states" : []
    }
    import_start = datetime.now().isoformat() # also id of import job

    resource_df = source('fdr_ckan', 'fdr_resource')
    #print(resource_df)
    resources = resource_df.to_dict(orient='records')
    #print(resources)
    #print(context.current_model.meta.get())
    #return

    resource_status_lines = []
    for resource in resources:
        resource_status_line = import_resource(resource, import_state)
        if resource_status_line is not None:
            resource_status_lines.append(resource_status_line)
            if resource_status_line['status'] == 'error':
                print('error, status :', resource_status_line)

    resource_status_df = pandas.DataFrame(resource_status_lines)
    print('import_resources resource_status_df', resource_status_df)
    resource_status_df['import_start'] = import_start
    resource_status_df['import_end'] = datetime.now().isoformat()
    print('import_resources out to current DBT schema')
    ## KO write_to_source(resource_status_df, resource_status_source, resource_status_table, mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
    write_to_source(resource_status_df, 'fdr_import', 'fdr_import_resource', mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
    # TODO LATER view & CSV in each data publishing org

    print('import_resources end')


import_resources()