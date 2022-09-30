# Projet dbt fdr-france-data-reseau

Projet des traitements globaux et des données mutualisées entre cas d'usage


## Provides

- scripts/ : import.py, publish.py (see there)
- macros :
    - setup (create_role_schemas, create_user),
    - union (of imported sources), conversion (from_csv, to_<type>.sql) and its UDFs, compute generic fields,
    - and dedupe, UDFs, FDR schemas...
- models :
  - périmètres de compétence géographique des collectivités (une table par cas d'usage, en pratique depuis geopackage ; mais sur FDR_SOURCE_NOM, TODO sur FDR_SOURCE rather than FDR_SOURCE_NOM)
  - population des communes 2022 (en pratique depuis CSV), aussi davantage de données démographiques de communes mais de 2014
  - INSEE ODS commune & region (en pratique depuis geojson),
  - metamodel indicators

## Rules

### tables ou vues produites par DBT (dans le schema eaupotable) :

- <FDR_USE_CASE>_raw_ : tables importées depuis les ressources CKAN par import.py
- *_src_*_parsed (vue) : union générique des différentes tables importées de chaque collectivité, avec conversion
automatique des champs vers leur _definition .sql
- *_src_*_translated (table) : (matérialise en table ou pas) unifie les différentes FDR_SOURCE_NOM (ex. Eau potable :
_en_service et _abandonnees), corrige (ex. Eau potable : 0-padding des codes) et enrichit localement
(des champs techniques : id unique global reproductible...)
- *_std_* (vue) : simple raccourci vers la précédente
- => *_std_*_labelled (vue) : l'enrichit des labels des codes
- _unified : unifié entre différentes FDR_SOURCE_NOM
- _deduped : dédupliqué
- _enriched : l'enrichit par exemple des communes, population / démographie...
- _kpi_ : indicateurs

### prefix

version with and without type-specific prefix

### generic fields

- src_kind : the processing kind / type, so is (ideally would be a subset of ex. apcom_birdz) FDR_SOURCE_NOM (ex. apcom_equip_birdz)
- src_name : makes src_id unique, so merely contains FDR_SOURCE_NOM and data_owner_id
- import_table / src_table : only for linking purpose since contains fields that are also available (use case, FDR_SOURCE_NOM, data_owner_id...)
- src_priority : must also be enough for src_id to be unique (so that both are enough in order_by_fields), so merely contains src_name
- src_id : the original id provided in the source
- id : unique id across all data_owner_id and FDR_SOURCE_NOM
- uuid : reproducible UUID that is generated from id

### Other chosen best practices

geo :
4326 by default (ST_Contains ; so also indexed),
but also _2154 to compute distances in meters (not indexed because does not improve performance)

as much as possible (materialized) as SQL views, except :
- EITHER _unified or _translated (then with indexes),
- and _deduped (to store dedup results ; and which must also run on indexed data)

which fields are kept :
imported table-specific fields are kept as much as possible,
__src fields are also provided to help debug conversion (in debug mode),
but both are skipped if they become too much (ex. twice slower with 350 fields ! there a column oriented DB becomes
appropriate), typically across several sources so in _unified

Nifi-ization flow / "always on" / on the fly / incremental :
TODO

Preventing DBT from dropping relations on which views created in Superset depend :
as in fdr_src_population_communes_typed :
WARNING made incremental, else DBT cascade drops dependent views created in Superset
i.e. only created if does not yet exist, though filled everytime (so must have a unique_key)
(TODO LATER macro that empties them on-run-start)
bonus : if run with is_incremental, only fills the ones with a newer last_changed
NB. alternatives : put them in DBT (!), enabled=false, or make them as tables (filled by Nifi)
see https://github.com/dbt-labs/dbt-core/issues/2185


## Install, build & run

**IMPORTANT** après dbt deps, remplacer le contenu de dbt_packages/dbt_profiler par celui de https://github.com/ozwillo/dbt-profiler (attention au "_", ne sera plus nécessaire quand une nouvelle version aura été publiée incluant https://github.com/data-mie/dbt-profiler/pull/38 )

### Install : DBT (1.2.1), fal (0.5.2), ckanapi

comme à https://docs.getdbt.com/dbt-cli/install/pip :
(mais sur Mac OSX voir https://docs.getdbt.com/dbt-cli/install/homebrew )

```shell
# prérequis python :
sudo apt-get install git libpq-dev python-dev python3-pipsudo apt-get remove python-cffi
sudo pip install --upgrade cffipip install cryptography~=3.4
# installer le venv de la version de python : (testés : python3.8-venv, python3.10-venv)
sudo apt install python3.10-venv

python3 -m venv dbt-env
source dbt-env/bin/activate
pip install --upgrade pip wheel setuptools
pip install dbt-postgres

pip install fal
pip install ckanapi
pip install requests
# for Excel import (pandas.to_excel()) :
pip install openpyxl
pip install xlrd

# mise à jour :
#pip install --upgrade dbt-postgres
# ou faire un nouveau venv et refaire la précédente procédure d'installation !
```

### Configuration

comme à https://docs.getdbt.com/dbt-cli/configure-your-profile :

```
mkdir /home/mdutoo/.dbt
vi /home/mdutoo/.dbt/profiles.yml
# nom du profile : fdr_votreorganisation(_votrebase)
# à adapter sur le modèle du profiles.yml fourni
```

Serveur PostgreSQL FDR (base partagée datalake ou dédiée) : demander

(optionnel) serveur PostgreSQL local :

Create "fdr" user :

    $> sudo su - postgres
    $> psql
    $postgresql> create user fdr with password 'fdr' createdb;
    $postgresql> \q

Create "fdr_datalake" database :

        $> psql -U fdr postgres -h localhost
        $postgresql> create database fdr_datalake encoding 'UTF8';

Now you should be able to log in to the database with the created user :

        psql -U fdr fdr_datalake -h localhost

### Build & run

```shell
source dbt-env/bin/activate
dbt deps # (une seule fois) installe les dépendance s'il y en a dans dbt_packages depuis https://hub.getdbt
dbt debug # (une seule fois) pour vérifier la configuration
dbt seed # (--full-refresh) quand les données d'exemple embarquées dans seeds/ changent
dbt run # (--full-refresh) pour réinstaller les models dans la base cible en tables et views
dbt test --store-failures # pour exécuter les tests (ET stocker les lignes en erreur dans le schema _test__audit) , d'une part génériques (de contraintes sur schémas configurées en .yml, notamment pour les modèles normalisés), et d'autre part spécifiques (requêtes dans des fichiers .sql sous tests/ renvoyant une ligne par erreur trouvée)
dbt docs generate
dbt docs serve # (--port 8001) sert la doc générée sur http://localhost:8000

# au-delà :
dbt run --target staging --select meta_indicators_by_type # un seul model SQL
dbt run test --store-failures # les lignes en erreurs des tests sont stockés (dans un schema _dbt_test__audit TODO mieux)

# debug :
vi logs/dbt.log

```

### Release

```shell
# en dev :
pip freeze > requirements.txt
# et y commenter la ligne pkg_resources sinon erreur No module named pkg_resources
# pas nécessaire, voir https://stackoverflow.com/questions/7446187/no-module-named-pkg-resources https://stackoverflow.com/questions/20635230/how-can-i-see-all-packages-that-depend-on-a-certain-package-with-pip

# en prod (après backup) :
# installer prérequis python et venv (voir plus haut)
python3 -m venv dbt-env
. dbt-env/bin/activate
pip install -r requirements.txt

cp profiles.yml ~/.dbt/profiles.yml
dbt deps
cd ../fdr-eaupotable
dbt deps
cd ../fdr-france-data-reseau
# NB. dbt seed est supposé déjà fait en prod pour tous ls projets une fois pour toutes depuis un poste de dev
./fdr_run_import_dbt_all.sh
# ou faire individuellement ce qu'il y a dans ce script
```

### DBT resources

Good primer tutorial https://www.kdnuggets.com/2021/07/dbt-data-transformation-tutorial.html

DBT 101 :
- snapshots : SCD2 that can be updated / run separately
- exposures : define outside uses in YAML, to publish doc and to run them separately
- metrics : defined in YAML, notably : time_grains=[day, week, month], dimensions=[plan, country], filters ; pour doc, MAIS ne produisent pas (de relation / model / macro) en elles-mêmes


### FAQ :

* column "appuiscommunssupp__Gestionnaire__None" does not exist
  14:24:34    HINT:  There is a column named "appuiscommunssupp__Gestionnaire__None" in table "appuiscommuns__supportaerien_indicators_region_ot", but it cannot be referenced from this part of the query.
  14:24:34    compiled SQL at target/run/fdr_appuiscommuns/models/exploitation/appuiscommuns__supportaerien_indicators_region_ot.sql
  => la structure du flux incrémental a changé par rapport à ce qu'il avait précédemment entré dans sa table historisée _ot, supprimer cette dernière (ou la migrer si on souhaite en garder les anciennes données)

* Unable to do partial parsing because a project config has changed
  => rm target/partial_parse.msgpack https://stackoverflow.com/questions/68439855/in-dbt-when-i-add-a-well-formatted-yml-file-to-my-project-i-stop-being-able-t

Gotchas - DBT :
- See test failures : store them in the database : dbt test --store-failures https://docs.getdbt.com/docs/building-a-dbt-project/tests https://github.com/dbt-labs/dbt-core/issues/2593 https://github.com/dbt-labs/dbt-core/issues/903
- index : https://docs.getdbt.com/reference/resource-configs/postgres-configs
- introspect compiled model : https://docs.getdbt.com/reference/dbt-jinja-functions/graph
- embed yaml conf in .sql : https://docs.getdbt.com/reference/dbt-jinja-functions/fromyaml
- dbt reuse : macros, packages (get executed first like they would be on their own including .sql files, but can pass different variables through root dbt_project.yml (?) ; TODO Q subpackages ?) https://www.fivetran.com/blog/how-to-re-use-dbt-guiding-rapid-mds-deployments
- run_query() must be conditioned by execute else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute
- run_query() of write statements must be followed by a "commit;" ! (or a ";" ?)  https://docs.getdbt.com/reference/dbt-jinja-functions/run_query

Gotchas - Jinja2 :
- doc https://jinja.palletsprojects.com/en/3.0.x/templates
- map() filter returns string "<generator object do_map at 0x10bd730>" => add |list https://github.com/pallets/jinja/issues/288
- change the value of a variable (esp. in a loop to find something) : not possible (and not in the spirit). But if really required, use a dict:  https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop
- macros accept other macros as arguments https://stackoverflow.com/questions/69079158/can-dbt-macros-accept-other-macros-as-arguments
- error The object ("{obj}") was used as a dictionary. This capability has been removed from objects of this type. => string utilisée en tant que list

Gotchas - DBeaver :
- a big query (with WITH statement...) throws error : DBeaver uses ";" character AND empty lines as statements separator, so remove these first https://dbeaver.io/forum/viewtopic.php?f=2&t=1687
- sometimes,

Gotchas - PostgreSQL :
- HINT:  No function matches the given name and argument types. => add explicit type casts to the arguments
- FAQ postgres blocks & logs says WARNING:  there is already a transaction in progress => try restarting DBeaver (see above), or else terminate all running queries :
  SELECT pg_cancel_backend(pid) FROM pg_stat_activity WHERE state = 'active' and pid <> pg_backend_pid();
- drop all tables in a schema :
```sql
DO $$
DECLARE
row record;
BEGIN
FOR row IN SELECT * FROM pg_tables WHERE schemaname = 'eaupotable'
LOOP
EXECUTE 'DROP TABLE eaupotable.' || quote_ident(row.tablename) || ' CASCADE';
END LOOP;
END;
$$;
```

Gotchas - FAL :
- either setup one fal script ex. publish.py on EACH model, or a single script declared at <schema>.yml top level
- fal run --all (or --select ...) ; else runs no script
- access dbt context variables beyond what fal provides (ex. target, graph...) :
    - either using execute_sql() : schema = execute_sql("select '{{ target.schema }}'").values[0][0]
    - or using fal as a python lib from a .py file
- run dbt macro : either using execute_sql() (or in dbt model or hook such as on-run-start/end and run it all using fal flow run)
- peut désormais exécuter aussi avant (--)before, mais pas encore de macro dbt
- passing arguments :
    - local and public : as dbt "metas" at the location of the script declaration accessed by
    - BUT global or secret : as OS env vars accessed through python ( https://github.com/fal-ai/fal/tree/main/examples/slack-example ; rather than env_var("DBT_ENV_SECRET_...") which is only accessible in profiles/packages.yml see https://docs.getdbt.com/reference/dbt-jinja-functions/env_var https://github.com/dbt-labs/dbt-core/issues/2514 )
- airbyte :)