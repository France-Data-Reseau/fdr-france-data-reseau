# Projet dbt fdr-france-data-reseau

Projet des traitements globaux et des données mutualisées entre cas d'usage

See Install, build & run and FAQ / Gotchas at the bottom.

Regular (incremental) run :
```bash
dbt run --target prod --select fdr.src fdr.perimetre.src tag:incremental
```

## Provides

- models :
  - *_std_perimetre(_all) : périmètres de compétence géographique des collectivités (une table par cas d'usage, en pratique depuis geopackage ; mais sur FDR_SOURCE_NOM, TODO sur FDR_SOURCE rather than FDR_SOURCE_NOM)
  - fdr_src_population_communes_typed : population des communes 2022 (en pratique depuis CSV), en table incrémentale ; aussi davantage de données démographiques de communes mais de 2014
  - fdr_std_communes_ods, fdr_std_regions_ods : INSEE ODS commune & region (en pratique depuis geojson), enrichies de leur population, en table incrémentale
- governance :
  - fdr_import_resource : résultat des imports en base par import.py (de geopackage...)
  - meta_indicators* : metamodel indicators
- macros :
  - setup (create_role_schemas, create_user),
  - union (of imported sources), conversion (from_csv, to_<type>.sql) and its UDFs, compute generic fields,
  - generic commune n-n linking, identity dedupe
  - create_views_per_data_owner (not used)
  - and UDFs, FDR schemas...
- scripts/ : import.py, publish.py (see there)


## Production deployment

- on the Nifi server, clone / checkout / pull all DBT projects with their latest version
- first deploy in _test schemas (including depending DBT projects), by running DBT processing fully
  (in --full-refresh mode, with --target test), by running manually commands that are in fdr_reset_run_import_dbt_all.sh
  but with the "test" target
- there check visually (ex. in DBeaver CE) outputs that are used outside,
- then switch key Superset charts to use them,
  - if they don't exist yet, add them in the Datasets page, in another configuration of the same database but with the name "TEST ...")
  - in the Datasets page, click on the Edit symbol on the right, there in the Colonnes tab click on Synchronizer les colonnes de la source
  - in each key chart, on the top left click on the three stacked dots near the dataset, there choose Changer le jeu de données
  - then click on Update chart, and if it displays OK on Enregistrer > Save (Overwrite / Ecrase)
- then switch external SQL views to use them (*)
- if OK, then in prod schema : idem ; if any problems, let / put Superset charts and external views on _test datasets.
  NB. outside platform-processing code (Eau potable, Eclairage public) should also output such a test version in the _test schema

(*) external views to create after deployment :

```sql
-- as user stellio :
create view stellio.point_lumineux_indicateurs_habitants_eclairage_public as (
SELECT count(reference) as nombre_point_lumineux, sum(puissance) as puissance_totale, gestionnaire_title, upper(unaccent("Libellé")) as est_dans_commune_com_nom, "Population municipale 2019"
from stellio.pointlumineux_eclairage_public
inner join "france-data-reseau"."fdr_src_population_communes_typed" on "Code" = insee::TEXT
group by gestionnaire_title, "Libellé", "Population municipale 2019"
);
-- test it :
select * from stellio.point_lumineux_indicateurs_habitants_eclairage_public
```

TODO remove doc about stellio.point_lumineux_indicateurs_habitants_eclairage_public once Superset uses rather
the DBT managed one (eclpub_kpi_point_lumineux_indicateurs_habitants)


## Rules

#### Cycle de vie et stabilité des relations / incrémentalisation :

L'incrémentalisation des traitements DBT est mise en place. Elle permet de ne traiter que les nouvelles données
(selon les métadonnées de date de modification CKAN), ce qui accélère donc les traitements réguliers,
et les rapproche de l'approche Nifi de traitement en flux plutôt qu'en masse,
et enfin ne supprime plus à chaque fois les tables ou vues utilisées de l'extérieur
(ce qui risquerait de supprimer des vues en dépendant).
Elle est accompagnée de règles claires pour toute relation PostgreSQL gérée par DBT :
- tout ce qui est traduction des imports d'est que vues (nommées *_src_*), mises à jour à chaque traitement régulier
- les données unifiées, mais aussi les autres données utilisées de manière externe à savoir les rapprochements (des communes)
  et déduplications, sont en tables remplies de manière incrémentale, donc stables et jamais recréées (donc jamais supprimées)
- les données enrichies (les autres *_std_*) et les KPIs (nommées *_kpi_*) sont en vues, et exclues des traitements réguliers,
  donc jamais supprimées.

### Règles de nommage des tables ou vues produites par DBT selon leur position dans le cycle de traitement :

(dans les schemas "france-data-reseau(_test)", appuiscommuns(_test) eaupotable(_test))

Celles à utiliser de l'extérieur, car stables et performantes, sont :
- les *_kpi_*,
- ou pour produire soi-même des KPIs, les *_std_*_enriched (ou _labelled)

- <FDR_USE_CASE>_raw_ : tables importées depuis les ressources CKAN par import.py
- *_def_*_ : définition SQL des modèles de données (colonnes et leurs types), à partir d'exemples statiques...
- *_src_*_parsed (vue) : vue SQL union générique des différentes tables importées de chaque collectivité,
  - avec conversion  automatique (des champs vers leur type dans la _definition .sql...).
  - Cette vue doit est recréée pour inclure tout nouveau  fichier / table importée.
  - N'y mettre que la macro d'union générique pour faciliter le débogage (de chaque champ converti avec sa version _src...).
- *_src_*(_translated) (table) : règles de traduction spécifiques depuis une FDR_SOURCE_NOM de modèle de données
différent vers le modèle normalisé ; TODO NON (matérialise en table ou pas) unifie les différentes FDR_SOURCE_NOM
(ex. Eau potable : _en_service et _abandonnees), corrige (ex. Eau potable : 0-padding des codes) et enrichit localement
(calcul des champs génériques), voir plus bas (des champs techniques : id unique global reproductible...)
- *_std_*(_unified) : unifié entre différentes FDR_SOURCE_NOM.
  - Il est conseillé qu'elle soit stockée en table, afin d'offre de meilleure performances aux vues qui en dépendent
(directement ou non, notamment celles de kpis / indicateurs),
  - et même en incrémental pour des traitements plus rapides même avec de nombreuses collectivités
participant au cas d'usage, mais aussi qu'ils ne détruisent pas avec elle les vus qui en dépendent (y compris
indirectement si les vues de ce projet qui ne sont pas inclues traitement DBT, typiquement pour n'inclure que les _src_
et incrémentales : dbt run --target prod --select apcom.src tag:incremental)
- *_std_*_dedupe_candidates : doublons trouvés dans les donnée unifiées. Table, conseillé incrémental
- *_std_*_deduped : vue dédupliquée des données unifiée, en appliquant des règles au précédents candidats.
- *_std_*_commune_linked : table de la relation n-n ici avec la table commune, bâtie par rapprochement. Table, conseillé incrémental
- => *_std_*_enriched : vue qui enrichit les données unifiées, par jointure par exemple :
  - avec les labels de codes de valeur non lisble / sémantiquement significative (*_labelled),
  - entre les différents types unifiés,
  - avec les communes (typiquement à l'aide de la table de la relation n-n issue de rapprochements),
  - avec leur population / démographie...
- => *_kpi_* : indicateurs sur les données enrichies, sans agrégation (laissée à Superset, conseillé quand possible i.e.
  dans Superset Pie Chart ou Pivot table)
- *_kpi_*_commune_owner : indicateurs sur les données enrichies agrégées par exemple par commune et collectivité (data_owner_id).
Requis pour Superset Bar Chart des différentes valeurs prises par un champ.

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

### Analyse du cycle de vie et stabilité des relations / incrémentatlisation

(pour Nifi-ization flow / "always on" / on the fly / incremental, et performance)

- d'abord, il faut exécuter les *_src* à chaque changement, afin que leurs vues _parsed incluent le cas échéant les
nouvelles tables importées de nouveaux fichiers ou leur conversions changées
- ensuite,
  - pour la performance des requêtes il faut que les données soient matérialisées en table par la suite,
  - pour ne pas trop en avoir il faut le faire quand même assez tôt => idéalement au-dessus de _src_*_translated
donc _std_*_unified
  - et pour la performance des traitements il faut ne traiter que les données changées, donc sur la dimension temporelle
en incrémental (PLUS TARD aussi seulement le type de traitement approprié ex. à un FDR_SOURCE_NOM précise,
mais la performance même à l'échelle des collectivités ne devrait pas le requérir)
- de plus, pour éviter de détruire les vues externes dépendant des relations DBT, il faut
  - que TOUTES les tables utilisées de l'extérieur (même indirectement par vue) soient incrémentales
=> mettre en tables incrémentales les *_std_*_unified, ainsi que les relations contenant les résultats des rapprochements ou doublons trouvés
NB. cela signifie aussi leur donner une unique_key et rajouter un filtre typiquement sur le champ last_changed pris des métadonnées CKAN
  - ET que les vues DBT en dépendant (même indirectement par vue) soient exclues du traitement DBT non complet
=> que ne soient traitées que les _src et les _std qui sont incrémentales
- pour faire un traitement non complet :
  - en ayant taggé "incremental" lesdites _std_ qui sont en incrémental,
  - dbt run --target prod --select fdr.src fdr.perimetre.src tag:incremental


### Other chosen best practices

geo :
4326 by default (ST_Contains ; so also indexed),
but also _2154 to compute distances in meters (not indexed because does not improve performance)

as much as possible (materialized) as SQL views (helps nifi-ization, but also performance because storing fields takes time),
except :
- EITHER _unified or _translated (then with indexes),
- either _deduped or _dedupe_candidates (to store dedup results ; and which must also run on indexed data)

which fields are kept :
imported table-specific fields are kept as much as possible,
__src fields are also provided to help debug conversion (in debug mode),
but both are skipped if they become too much (ex. twice slower with 350 fields ! there a column oriented DB becomes
appropriate), typically across several sources so in _unified

TODO si pbs perfs, mettre champ dans table voire incrémental ex. ods communes population

Preventing DBT from dropping relations on which views created in Superset depend :
as in fdr_src_population_communes_typed :
WARNING made incremental, else DBT cascade drops dependent views created in Superset
i.e. only created if does not yet exist, though filled everytime (so must have a unique_key)
(TODO LATER macro that empties them on-run-start)
bonus : if run with is_incremental, only fills the ones with a newer last_changed
NB. alternatives : put them in DBT (!), enabled=false, or make them as tables (filled by Nifi)
see https://github.com/dbt-labs/dbt-core/issues/2185

Working with example data :
- if the "use_example" variable is true in dbt_project.yml, _translated steps may use an example source model rather than the regular data
(i.e. the _parsed step with the union of imported tables with the same FDR_SOURCE_NOM).
For now only in apcom non native sources.
- to avoid static data files (examples and their expected...) having empty values being interpreted by DBT as their
column being of int4 type, fill all of the first line's column values (rather than setting column_types in the seeds
part of _schema.yml)

Other disabled by default features and examples :
  enableArrayLinked: false # in apcom
  enableOverTime: false # sinon problèmes, voir dans les exploitation/*_ot.sql
  enableProfiling: false


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
dbt run --target test --select meta_indicators_by_type # un seul model SQL, dans un schema ..._test
dbt run --target test --select meta_indicators_by_type+ # ce model ET tous ses descendants (FORTEMENT CONSEILLE
# car souvent pour le rebâtir il est supprimé et donc toutes les vues qui en dépendent aussi !)
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
# et les compléter avec les informations de connexion

cd ../fdr-france-data-reseau
# la première fois :
./fdr_reset_run_import_dbt_all.sh
# par la suite :
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
- dbt_utils.pivot() :
  - if using get_column_values(), do it not on source_model (in kpi often an inefficient view) but on an efficient table (ex. _unified)
  - NB. outputs a lot of dummt logs ('escape_single_quotes' obsolete macro)

Gotchas - Jinja2 :
- doc https://jinja.palletsprojects.com/en/3.0.x/templates
- map() filter returns string "<generator object do_map at 0x10bd730>" => add |list https://github.com/pallets/jinja/issues/288
- change the value of a variable (esp. in a loop to find something) : not possible (and not in the spirit). But if really required, use a dict:  https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop
- macros accept other macros as arguments https://stackoverflow.com/questions/69079158/can-dbt-macros-accept-other-macros-as-arguments
- error The object ("{obj}") was used as a dictionary. This capability has been removed from objects of this type. => string utilisée en tant que list

Gotchas - DBeaver :
- a big query (with WITH statement...) throws error : DBeaver uses ";" character AND empty lines as statements separator, so remove these first https://dbeaver.io/forum/viewtopic.php?f=2&t=1687
- sometimes, a relation can't be refreshed : it might be because a change has been made to it in the table view of
DBeaver (maybe mistakenly), but not persisted. In this case, close said table view and click to cancel or persist the change.
- doc :
- screenshots
- outil SQL ex. DBeaver,
- guide / tutorial plus précis de techniques pour comment lire / analyser / introspecter, parser, nettoyer (du mail à Etienne), retyper, normaliser, vérifier une source
- manuel de comment bien adopter / adapter / (ré)utiliser le(s) projet(s) dbt de base / exemple et mutualisés
- TODO outiller : génération de CSV (ex. _expected.csv..., mais plutôt pas __definition.csv qui doit être fabriqué et non réel) depuis relation SQL model DBT de source nettoyée voire unifiée
- Exporter une relation vers CSV :
  - sur les données, bouton droit > Exporter les résultats... et dans la fenêtre modale : Exporter en fichier CSV,
  - Suivant : Fetch size = 5 (pour un exemple, ou ex. 100000 pour tout),
  - Suivant : optionellement changed Séparateur à ; si on préfère Excel,
  - Suivant : si besoin changer le nom du fichier, Suivant : commencer

Gotchas - PostgreSQL :
- UNION without ALL removes duplicates lines according to the columns of the first column statement
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


## Development notes

### Nommage

- FINAL :
  - apcom_birdz_type_src est la dbt source qui unit (hors dbt donc) toutes les données source au format birdz
  - apcom_def_type_src est la dbt source qui unit (hors dbt donc) toutes les données source au format natif
- type
- partitionnement (qui peut être type de source voire source avant normalisation, divers enrichissement pour divers usages après)
- (date : oui est une "version" mais pas un partitionnement, en général elle est DANS la donnée, à moins d'être un snapshot figé, car si pas figé est une branche et donc pas vraiement une date)
- workflow, étape de, majeure : peuvent en être des indicateurs / guides. Ils sont :
  - "source" (décliné par source : megalis... TODO native, CSV). Les éléments discriminants sont le type fourni, et si nécessaire le traitement appliqué, voire le type source (SI un type normalisé provient de plusieurs types sources, qui sont alors peut-être autant de sous-types de sources).
  - "normalisation" sur sa partie définition, unification et déduplication. Les éléments discriminants sont le type fourni, puis l'étape appliquée.
  - "exploitation" / usage (indicateurs / kpi, mais sans doute pas la version CSV, geopackage, geoserver). Les discriminants sont :
    - le concept support de la métrique qui est souvent un type,
    - la métrique (linéaire de canalisation, poteau électrique ou technologie d'équipement...) MAIS le plus souvent cette relation concept suffit à fournir beaucoup / toutes les métriques (par des group by différents en dataviz ex. superset),
    - puis si nécessaire les dimensions (territoire reg/dep/commune qui est une hiérarchie multiple avec AODE, éventuellement métier ex. sur/sous-types de matériau ; mais tout cela est de l'enrichissement) et leurs grains offerts, y compris temporelle (là il peut y avoir des choses à faire : générer des jours ou avoir préparé un historique en SCD2 / DBT snapshot)    - Les enrichissements sont des suppléments de "normalisation" ou (que/et) des requis de "exploitation".
- Ils peuvent ainsi être classés dans des dossiers : apcom (normalisation, qui est aussi le dossier du projet), (apcom/)(src ou source/)megalis..., (apcom/)(exploitation/)kpi(/kpi1 ex. d'occupation). Cette classification peut être utilement reprise en préfixe de relation SQL / model DBT :
  - apcom_(src_)osm_supportaerien(_translated,deduped), apcom_std_supportaerien(_unified,deduped), apcom_(use_)kpi_suivioccupation_day
