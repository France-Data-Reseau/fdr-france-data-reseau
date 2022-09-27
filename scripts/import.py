'''
Import (incrémental) des données des ressources CKAN selon les champs personnalisés FDR_ dans la base SQL datalake
- se base sur la table public.fdr_resource de toutes les ressources CKAN ayant des champs personnalisés FDR configurés sur leur jeu de données.
A synchroniser (ex. automatiquement par Nifi ou manuellement par DBeaver-CE) depuis la vue fdr_resource.sql à installer
dans la base CKAN.
Cette méthode est plus efficace que de nombreux appels à l'API CKAN.
- pour chacune :
  - validation : de ses champs personnalisés FDR,
  - incrémental : téléchargement et insertion que si changement depuis le dernier import réussi,
  - téléchargement depuis CKAN par son API
  - puis, si FDR_TARGET!=archive, import selon le format.
- formats supportés :
  - geopackage, geojson, shp (par exécution de la commande ogr2ogr de GDAL, pouvant être dockérisée).
D'autres formats géographiques peuvent l'être facilement par la même méthode.
    - SHP FAQ - error "Unable to open datasource" : Shapefile should be a ZIP with AT ITS ROOT DIR 4 four files with the same
name and a different extension : .dbf, .prj, .shp, .shx. See example that works at https://gadm.org/download_country.html .
    - For the administrator : import.py working dir must not have space in its path (else ogr2ogr error error "Unable to open datasource",
see https://gis.stackexchange.com/questions/70315/handling-file-names-with-spaces-in-ogr2ogr).
  - CSV, xlsx, xls : par pandas et DBT (aidé de fal et fal_ext.py), vers des champs texte seulement.
On le préfère à la visibilisation des tables importées par CKAN dans sa base Datastore, qui dépend trop de CKAN et fournit
des types de champs des fois faux et pas trivialement contrôlables.
- cas de resource externe i.e. non chargées depuis fichier mis en ligne, mais depuis url :
pas possible car pas de flux "changed" et / ou obligé de télécharger à chaque fois pour savoir), bref acceptable que si pas souvent ex. 1/j, sinon ajouter vrai moissonnage (pas prio), ou dire de modifier la resource manuellement quand on veut dire qu'elle a changé
- résultats et erreurs écrits dans la table france-data-reseau.fdr_import_resource
  - mise à disposition auprès des collectivités en CSV dans CKAN  dans l'organisation FDR par le script publish.py
  - et surtout, qui sert de référence à tous les traitements exploitant les données importées pour en retrouver les tables et leur configuration (notamment du type de traitement à appliquer, par FDR_SOURCE_NOM), qu'ils soient en DBT / SQL ou en python.
- NB. dans le cas de traitements DBT :
  - c'est la macro fdr_source_union_from_name() qui unifie toutes les tables ayant une FDR_SOURCE_NOM données, à appeler depuis un model DBT <préfixe cas usage>_src_<FDR_SOURCE_NOM>_parsed.sql
  - elle effectue une première conversion générique des champs vers les types définis dans un model DBT <préfixe cas usage>_def_<type en général FDR_SOURCE_NOM>_definition.sql (par exemple sdirve_def_indicateurs_definition.sql), elle-même basée sur la conversion SQL spécifique d'un exemple minimal CSV statique, et pouvant comprendre une mise en correspondance (mapping) des noms des champs
  - après quoi peut être effectué tout complément de normalisation dans un model _translated, d'enrichissement (labels des codes), rapprochement géographique (avec communes ou entre types), déduplication, préparation d'indicateurs / kpis...
- amont : se démarre par fal ou directement python
- aval : le champ last_changed column autorise ex. nifi tde détecter de nouvelles données et les traiter.
TODO :
- support du "dictionnaire de données" utilisé par Eau potable par mapping avancé et traduction des codes guidée ?
- exécution plutôt une fois par projet DBT cas d'usage ? ou au contraire plus simplement même de leurs traitements en une seule fois dans un projet DBT qui dépend de tous ?

OBSOLETE introspect CKAN API to :
- build list of datastore-imported files (ex. FDR_ROLE=source, format=gepackage) to make visible as views using dbt on-run-start macro
- download other files that have to be imported, and import them :
  - geojson (FDR_ROLE=perimetre), using COPY ?? NO have to add the organization_name OR RATHER org.SIREN (from org extra fields / custom keys) !
  - geopackage (FDR_ROLE=source, format=geopackage), using dockerized ogr2ogr (or in Airflow ??) (TODO only if newer udpate date)
- import only those that have changed (according to the local copy of fdr_resource), unless deleting the import state file 'import_state.json'
example :

rajouter dans des jeux de données de cas d'usage
  - avec les champs personnalisés requis :
    - exemple : FDR_CAS_USAGE=eaupotable, FDR_ROLE=source, FDR_SOURCE_NOM=canalisations_en_service, FDR_TARGET=prod
  - et dedans créer des ressources en mettant en ligne des fichiers de données
    - ayant les champs impliqués par le modèle de données correspondant défini dans le JDB

cd fdr-france-data-reseau/

# install :
# - python : see README (python venv with dbt, fal, ckan, requests, for excel import openpyxl)
# - datalake structure init (or update) - create use case roles and (shared, not personal) schemas with rights :
# must be run as DB admin (or a user having permission to create role and schema). NB. First runs create_udfs because it requires it.
# NB. also adds required postgres extension uuid-ossp
dbt run-operation create_roles_schemas --target prod_(pg)admin(_stellio)
- create a DBT admin user and use it from now on, i.e. in DBT configure it in prod(_stellio) profile :
set -a ; PASSWORD=`openssl rand -base64 ${1:-16}` ; dbt run-operation create_user --args '{name: "dbt_admin_user", schemas_string: "appuiscommuns,eaupotable,sdirve,eclairage_public,france-data-reseau"}' --target prod_(pg)admin(_stellio) ; set +a ; echo password : $PASSWORD
#openssl rand -base64 ${1:-16}
#CREATE USER "dbt_admin_user" IN GROUP "dbt_admin" PASSWORD 'Pp2UCofx6G/+fDGe9wX5Kg==' CREATEDB;

# - CKAN resource data sync :  create view in ckan database (scripts/fdr_ckan_resource.sql),
# then sync it to the "france-data-reseau".fdr_ckan_resource relation, either manually using ex. DBeaver-CE tasks
# or automatically using ex. Nifi.
# For Nifi and other solutions that require a table with PK index, here is a helper to create it :
dbt run-operation create_fdr_ckan_resource_nifi --target prod_pgadmin_stellio
# NB. import.py should not require anything else.
# If current (prod_admin) schema was (required by on-run-start create_udfs and as said import.py.) was not created
# by previous create_roles_schemas DBT operation, it could be created as follows, which would need DB admin rights
# and at least one selected model :
#dbt run --select meta --target prod_admin(_stellio)


set -a ; PASSWORD="somepassword" ; dbt run-operation create_user --args '{name: "francois@datactivi.st", schemas_string: "appuiscommuns,france-data-reseau"}' --target prod_pgadmin_stellio ; set +a ; echo password : $PASSWORD
set -a ; PASSWORD=`openssl rand -base64 ${1:-16}` ; dbt run-operation create_user --args '{name: "francois@datactivi.st", schemas_string: "appuiscommuns,france-data-reseau"}' --target prod_pgadmin_stellio ; set +a ; echo password : $PASSWORD


# import CKAN resource files in DB and create meta / report table :
# must be run as datalake admin (imports table in all use case schemas).
# params have to be provided in environment variables (see env.template & README ; set -a propagates them to subshells / commands) :
set -a ; source env.prod(_stellio) ; set +a
fal run --target prod_admin(_stellio) --before
# or (python launching faldbt)
cd scripts
python import.py prod_admin(_stellio)

# OLD publish :
dbt run --target prod --select fdr_import_resource_view
fal run --select all --target prod

# create views UNIONing imported tables of the same FDR_SOURCE_NOM :
NON dbt run-operation create_views_union --target prod_sync, plutôt DANS un projet cas d'usage, par exemple :
cd fdr-eaupotable/
dbt run --target prod --select eaupot_src_canalisations_en_service_parsed

or in test :
set -a ; source env.test ; set +a
cd fdr-france-data-reseau/
# IF NOT CREATE by previous command, create current (prod_sync) schema for fal import or on-run-start create_udfs (at least one model is required) :
dbt run --select meta --target prod_sync
fal run --target prod_sync_test --before
cd fdr-eaupotable/
dbt seed --target prod_stellio
dbt run --target test --select eaupot_src_canalisations_en_service_parsed
'''



import os
from ckanapi import RemoteCKAN, NotAuthorized

# CKAN configuration (to download files) :
ua = 'ckanapifdr/1.0 (+https://ckan.francedatareseau.fr)'
fdrckan_url = os.getenv("FDR_SYNC_CKAN_URL")
#fdrckan_url = 'https://ckan.francedatareseau.fr'
#fdrckan_url = 'http://172.18.0.14:5000'
fdrckan_apikey = os.getenv("FDR_SYNC_CKAN_API_KEY")
fdrckan = RemoteCKAN(fdrckan_url, apikey=fdrckan_apikey, user_agent=ua)

ogr2ogr_command_docker_prefix = os.getenv("FDR_SYNC_OGR2OGR_COMMAND_DOCKER_PREFIX", "docker run --rm --net=host -v /home:/home")
ogr2ogr_command_docker_image = os.getenv("FDR_SYNC_OGR2OGR_COMMAND_DOCKER_IMAGE", "osgeo/gdal:alpine-small-latest")
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
# (ex. get usage-* orgs and their FDR_USE_CASE as prefix and schema)
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
    'fdr' : {
        'use_case_prefix' : 'fdr',
        'schema' : 'france-data-reseau'
    }
}

# dev test :
#id_aode = 'sireneaudazur'
#FDR_SOURCE_NOM = 'canalisation' # TODO native canalisation
#step = 'imported'
#source_file = "/home/mdutoo/dev/ozwillo/fdr/data/EAU D'AZUR/2022-03-09_802630608_canalisations_en_service.gpkg"
#appuiscommuns_test.eaupot_src_native_sireneaudazur_canalisation
#source_file = '.geojson'

# data completing hacks :
# if no FDR_SOURCE_NOM, uses the first of the following ones if the resource name contains it
fdr_source_noms = ['canalisations_en_service', 'canalisations_abandonnees', 'reparations',
                   #'pointlumineux', 'dictionnaire_champs_valeurs'
                   ]

# NOT USED
def get_from_ckan():
  found_packages = fdrckan.action.package_search(q='+FDR_CAS_USAGE:"' + FDR_CAS_USAGE + '"', include_private=True)
  print(found_packages)


import sys
import json
import os, subprocess
import requests
import re
from pathlib import Path
from datetime import datetime
import pandas


# for write CSV & other( pandas imported) formats using dbt (& fal) :

# check if started by fal CLI, else complete init :
# see https://www.oreilly.com/library/view/python-cookbook/0596001673/ch17s02.html
try: write_to_source
except NameError: launched_by_fal = False
else: launched_by_fal = True

if launched_by_fal:
    from scripts.faldbt_ext import write_table
    project_dir="."
    profile_target = execute_sql("select '{{ target.name }}'").values[0][0]
else:
    from faldbt_ext import write_table
    project_dir=".."
    profile_target = sys.argv[1] if len(sys.argv) >= 2 else None # prod_sync prod_admin

from faldbt.lib import (_get_adapter) # _execute_sql # _write_relation # _existing_or_new_connection, _connection_name, _create_engine_from_connection
from fal import FalDbt
profiles_dir = "~/.dbt"
faldbt = FalDbt(profiles_dir=profiles_dir, project_dir=project_dir, profile_target=profile_target)
config = faldbt._config
print("import - dbt config :", config.target_name)

model_for_connection = faldbt._source('fdr_import', 'fdr_import_resource')
adapter = _get_adapter(project_dir, profiles_dir, profile_target, config=config)
#write_table(test_relation, 'testwritedyn', schema, model_for_connection, adapter)

if not launched_by_fal:
    ref = faldbt.ref
    source = faldbt.source
    write_to_source = faldbt.write_to_source


#from dbt.logger import GLOBAL_LOGGER as logger

cache_dir = project_dir
import_state_file = cache_dir + '/import_state.json'



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
        #print('import not changed, skip in step', step, resource_key, import_state.get(resource_key), last_changed_string)
        pass
    else:
        print('import changed in step', step, resource, resource_key, import_state.get(resource_key), last_changed_string)
    return not has_stayed_same

def set_changed(resource, step='default'):
    last_changed_string = build_last_changed_string(resource)
    resource_key = build_resource_key(resource, step)
    import_state[resource_key] = last_changed_string
    with open(import_state_file, 'w') as f:
        json.dump(import_state, f, indent=4)
    print('import set_changed', resource_key, last_changed_string)

def build_resource_key(resource, step='default'):
    resource_key = '/'.join([resource['org_name'], resource['ds_name'], resource['name'], resource['id'], step, config.target_name])
    return resource_key

'''
NB. in case of external resource (no last_modified), the dataset's metadata_modified
is used only. So for the import to be updated, the dataset has to be manually modified.
'''
def build_last_changed_string(resource):
    last_modified = resource.get('last_modified')
    ds_metadata_modified = resource.get('ds_metadata_modified')
    last_changed = last_modified if not ds_metadata_modified else ds_metadata_modified if not last_modified else last_modified if last_modified > ds_metadata_modified else ds_metadata_modified
    # NB. in case of external url : OK is ds_metadata_modified because NaT < 2022-04-21T07:22:05 (?)
    last_changed_string = last_changed.isoformat() if last_changed else None
    # because else NotImplementedError: Don't know how to literal-quote value numpy.datetime
    # https://stackoverflow.com/questions/49490623/datetime-filtering-with-sqlalchemy-isnt-working
    return last_changed_string

'''
Downloads from CKAN a local copy at a path mirroring CKAN's
and including resource id to avoid conflicts,
AND without spaces in the file path (so neither must working dir path !) else ogr error "Unable to open datasource".
'''
def download_ckan_resource(resource):
    step = 'download'
    is_external_resource = pandas.isnull(resource['size']) # and NOT last_modified because even if in CKAN it's null, it's then
    resource_file_extension = resource['url'].rsplit('.', 1)[-1]
    # use resource id in name else may conflict ex. data.csv :
    source_file_path = os.path.sep.join([cache_dir, 'ckan', resource['org_name'], resource['ds_name'], resource['id'] + '.' + resource_file_extension])
    if not compute_has_changed(resource, step):
        # skip download
        return source_file_path

    Path(source_file_path).parents[0].mkdir(parents=True, exist_ok=True)
    internal_url = '/'.join([fdrckan_url, 'dataset', resource['ds_id'], 'resource', resource['id'], 'download', resource['url']])
    url = resource['url'] if is_external_resource else internal_url
    # replaced by ds_metadata_modified otherwise Nifi QueryDatabaseTable can't detect it !
    print('import download_ckan_resource', url, source_file_path, resource, is_external_resource, import_state.get(url))
    headers = {
        "Authorization": fdrckan_apikey
    }
    resp = requests.get(url, headers=headers)
    with open(source_file_path, "wb") as f:
        f.write(resp.content)

    set_changed(resource, step)
    return source_file_path


'''
- ogr problem with spaces in filename : https://gis.stackexchange.com/questions/70315/handling-file-names-with-spaces-in-ogr2ogr
- access zip including shapefile using /vsizip/ : https://gdal.org/user/virtual_file_systems.html
- when UnicodeDecodeError when decoding ogr2ogr process error, it is reexecuted with PGCLIENTENCODING=LATIN1 env var
being set, in an attempt to handle Windows-made files esp. Shapefile zipped files. This means that when NOT starting
ogr2ogr through its docker container, these files won't be able to be handled.
'''
def ogr2ogr(source_file, schema, table, resource, PGCLIENTENCODING=None):
    schema_and_table = schema + '.' + table
    is_zip = resource['url'].endswith('.zip')
    # see https://gis.stackexchange.com/questions/154004/execute-ogr2ogr-from-python
    is_ogr2ogr_docker_command = len(ogr2ogr_command_docker_prefix.strip()) > 0
    ogr2ogr_command_prefix = ogr2ogr_command_docker_prefix if is_ogr2ogr_docker_command else ""
    docker_env_vars = ["-e", "PGCLIENTENCODING=" + PGCLIENTENCODING] if PGCLIENTENCODING and is_ogr2ogr_docker_command else []
    command = ogr2ogr_command_prefix.split(' ') + docker_env_vars + [
          ogr2ogr_command_docker_image,
          "ogr2ogr",
          #"--config", "PG_LIST_ALL_TABLES", "YES",
          "-f", "PostgreSQL",
          "-overwrite", # else not append, and if nothing command blocks when already exists ?! https://lists.osgeo.org/pipermail/gdal-dev/2021-July/054422.html
          #"-append", "-doo", "\"PRELUDE_STATEMENTS=SET ROLE 'france-data-reseau'\"", # https://lists.osgeo.org/pipermail/gdal-dev/2021-July/054422.html
          # NB. should be PG:"..." but does not work here though it works manually
          "PG:host='" + host + "' port='" + port + "' user='" + user + "' password='" + password + "' dbname='" + database + "'",
          # for zip esp Shapefile :
          ('/vsizip/' if is_zip else '') + os.path.abspath(source_file), # else FAILURE: Unable to open datasource...
           "-nln", schema_and_table] # don't quote else they go in the name ! but ogr2ogr replaces ex. - by _...
    print(' '.join(command))
    try:
        res = subprocess.run(command, check=True, capture_output=True)

        # https://stackoverflow.com/questions/39563802/subprocess-calledprocesserror-what-is-the-error
        error_msg = str(res.stderr)
        is_error = "error" in error_msg.lower() # even though returncode=0 : stderr=b'ERROR 1: COPY statement failed.\nERROR:  invalid byte sequence for encoding "UTF8": 0xc9 0x56\nCONTEXT:  COPY apcom_raw_apcom_aat_gthdv2_252901145, line 1
        if is_error:
            print('import - error calling docker ogr2ogr :', error_msg)
            if 'invalid byte sequence for encoding "UTF8"' in error_msg:
                if not PGCLIENTENCODING:
                    print('   attempting to run docker ogr2ogr again with  PGCLIENTENCODING=LATIN1 :')
                    return ogr2ogr(source_file, schema, table, resource, PGCLIENTENCODING='LATIN1')
                else:
                    return 'import - while decoding docker ogr2ogr, too many UnicodeDecodeErrors'
            else:
                return error_msg
        else:
            return None

    except subprocess.CalledProcessError as e:
        # res.returncode != 0
        print('import - error calling docker ogr2ogr :', e)
        try:
            return e.stderr.decode("utf-8") # e.output ; if not decode, sqlalchemy typeError: a bytes-like object is required, not 'str'
        except UnicodeDecodeError as ude:
            print('import - while decoding docker ogr2ogr, UnicodeDecodeError', ude) # UnicodeDecodeError: 'utf-8' codec can't decode byte 0xc9 in position 437: invalid continuation byte
            if not PGCLIENTENCODING:
                print('   attempting to run docker ogr2ogr again with  PGCLIENTENCODING=LATIN1 :')
                return ogr2ogr(source_file, schema, table, resource, PGCLIENTENCODING='LATIN1')
            else:
                return 'import - while decoding docker ogr2ogr, too many UnicodeDecodeErrors'

def csv_to_dbt_table(source_file_path, schema, table, resource):
    try:
        # infer separator, and parse all fields as string see https://stackoverflow.com/questions/16988526/pandas-reading-csv-as-string-type
        parsed_file_df = pandas.read_csv(source_file_path, sep = None, dtype=str)
        # sheet_name, true/false_values, na_values/filter, dates, decimal : '.'...
        #write_to_source(parsed_file_df, 's', 'r', mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
        write_table(parsed_file_df, table, schema, model_for_connection, adapter)
        return None
    except Exception as e:
        print('Error importing CSV', e)
        return str(e)

def excel_to_dbt_table(source_file_path, schema, table, resource):
    try:
        # infer separator, and parse all fields as string see https://stackoverflow.com/questions/16988526/pandas-reading-csv-as-string-type
        parsed_file_df = pandas.read_excel(source_file_path, dtype=str)
        # sheet_name, true/false_values, na_values/filter, dates, decimal : '.'...
        #write_to_source(parsed_file_df, 's', 'r', mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
        write_table(parsed_file_df, table, schema, model_for_connection, adapter)
        return None
    except Exception as e:
        print('Error importing Excel', e)
        return str(e)

format_to_import_fct = {
    'gpkg' : ogr2ogr,
    'geopackage' : ogr2ogr,
    'geojson' : ogr2ogr,
    'shp' : ogr2ogr,
    'shapefile' : ogr2ogr,
    'csv' : csv_to_dbt_table,
    'xls' : excel_to_dbt_table,
    'xlsx' : excel_to_dbt_table
}
all_formats = [f for f in format_to_import_fct]
#print(all_formats)


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
    #print('import_resource', resource['name'], FDR_ROLE)
    if FDR_ROLE not in ['source', 'perimetre']:
        return

    data_owner_id_tmp = resource['fdr_siren'] if resource['fdr_siren'] is not None and len(resource['fdr_siren'].strip()) != 0 else resource['org_id'][:4] + resource['org_name'][-6:] # TODO FDR_SIREN
    # make it safe for postgres, else ogr2ogr does it (and doesn't allow quoting) :
    data_owner_id = re.sub('[^a-zA-Z0-9_]', '', data_owner_id_tmp.lower())
    # format : (CKAN format is always lower case)
    format = (resource['format'] if resource['format'] is not None else os.path.splitext(resource['name'])).lower()
    #print('import_resource', resource['name'], data_owner_id, format)

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
            messages.append({ "status" : "error", "text" : "while downloading, " + str(e) })
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
        print('import_resource', FDR_SOURCE_NOM, data_owner_id, format, resource['name'], schema, table, source_file_path)

        schema_and_table = schema + '.' + table
        if schema_and_table in import_state['schema_and_tables']:
            messages.append({ "status" : "error", "text" : "schema and table duplicate " + schema_and_table })
            step = "prepare"

        elif compute_has_changed(resource, 'insert'):
            import_state['schema_and_tables'].append(schema_and_table)

            import_fct = format_to_import_fct[format]

            if import_fct:
                # TODO in org schema ! and afterwards create view with schema, use_case_prefix
                #print('supported format', format, resource['name'])
                res = import_fct(source_file_path, schema, table, resource)
                step = import_fct.__name__

            else:
                import_fct = None
                res = "unsupported format"
                step = "prepare"

            if res:
                # escaping ex. double quotes : https://stackoverflow.com/questions/18886596/replace-all-quotes-in-a-string-with-escaped-quotes
                # else unparsable by SQL ::json ERROR: invalid input syntax for type json Détail : Token "0000" is invalid. Où : JSON data, line 1: ...ROR:  date/time field value out of range: \\"0000...
                #res_escaped = json.dumps(res) # NO same problem ?!
                res_escaped = res.replace('"', "'")
                messages.append({ "status" : "error", "text" : res_escaped })
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
        "last_changed": build_last_changed_string(resource),
        "resource_id" : resource['id'], "resource_name" : resource['name'],
        "dataset_id" : resource['ds_id'], "dataset_name" : resource['ds_name'], "dataset_title" : resource['ds_title'],
        "org_id" : resource['org_id'], "org_name" : resource['org_name'], "org_title" : resource['org_title'], # label
        "data_owner_id" : data_owner_id, "u_email" : resource['u_email'],
        "format" : format, "source_file_path" : source_file_path,
        "schema" : schema, "table" : table, "use_case_prefix" : use_case_prefix,
        "messages" : json.dumps(messages) # , indent = 4
    }

def import_resources(schema_suffix = ''):
    print('import_resources start')
    print('import_resources params:', fdrckan_url, ogr2ogr_command_docker_prefix, ogr2ogr_command_docker_image, host, port, database, user)
    print('import_resources conf:', fdr_cas_usages, all_formats, fdr_source_noms)
    if not fdrckan_url or not host or not port or not database or not user:
        raise Exception("import_resources missing params, abort")

    import_state = {
        "schema_suffix" : schema_suffix, ##
        "schema_and_tables" : [], # used to prevent duplicates
        "resource_states" : []
    }
    import_start = datetime.now().isoformat() # also id of import job

    resource_df = source('fdr_ckan', 'fdr_ckan_resource')
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
    print('import_resources resource_status_df', resource_status_df.sort_values(by=['status']))
    resource_status_df['import_start'] = import_start
    resource_status_df['import_end'] = datetime.now().isoformat()
    print('import_resources out to current DBT schema')
    ## KO write_to_source(resource_status_df, resource_status_source, resource_status_table, mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
    write_to_source(resource_status_df, 'fdr_import', 'fdr_import_resource', mode='overwrite') # also avoids changing schema pb with mode='append' https://docs.fal.ai/Reference/variables-and-functions
    # TODO LATER view & CSV in each data publishing org

    print('import_resources end')


import_resources()