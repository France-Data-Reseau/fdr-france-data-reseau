{#
Conversion automatique des champs d'une source, avec 2 modes :
- soit, si column_models est fourni, vers des champs cible fournis, typiquement une _definition SQL d'un modèle de données normalisé
- soit, sinon, vers le même modèle, mais avec une version dégradées de ses différentes fonctionnalités (voir plus bas).

Notamment utilisé par les macros d'union de sources importées en base différentes, et pour charger les données CSV
embarquées dans DBT (à l'exclusion de _exemple(_stg) car il créé précisément la _definition).

Fonctionnalités :
- le def_from_source_mapping fourni permet de faire correspondre une colonne cible à une colonne nommée
différemment dans la source, où elle est cherchée en préservant la casse, ou sinon en casse basse,
et s'il y a un dictionnaire de données dans son renommage correspondant
- si les informations d'un dictionnaire de données sont fournies, réalise la traduction des codes correspondante,
par jointures
- tous les types de champ cible sont parsés de manière souple / "lenient" depuis du textuel ou leurs formats compatibles
- champs cible géo : détectés par la geo_pattern fournie ET le type cible (DBT 'USER-DEFINED'). NB. pas possible de se
contenter de la geo_pattern car certains ne sont pas parsés (apcom_birdz avec french floating point, centre des
communes lon/lat), TODO à moins de fournir une fonction de conversion géo custom.
Parsing depuis geojson ou WKT selon wkt_rather_than_geojson fourni, avec le srid fourni.
Si best_geometry_columns est fourni, utilise à la place la première des autres colonnes sources ainsi nommées s'il y en
a. Permet de gérer le cas de plusieurs colonnes géo dans la source, ce que ne permettrait pas une simple
source_geo_pattern, et le cas commun d'une colonne géo source de nom généré par les outils d'export ou import.
TODO aussi sans column_models / _definition
- champs cible ARRAY : préservés (TODO LATER si pas ARRAY en source, array_to_json ?)
- champs cible uuid : détectés par la uuid_pattern fournie. A priori utile uniquement pour le champ technique _uuid (les
identifiants apcom n'étant finalement pas des UUIDs)
- champs cible number : les types source numériques (DBT is_number()) sont préservés, sinon parsing vers numeric (macro to_numeric_or_null() et ses UDFs))
- champs cible date : les types source date, timestamp, timestamp with time zone sont préservés, sinon parsing avec les date_formats
fournis (macro to_date_or_null() et ses UDFs))
- champs cible boolean : le type source boolean est préservé, sinon parsing depuis numeric ou sinon text
- sinon (dont textuels) : champs source convertis en text

parameters :
- source : a dbt model (from ref() or source()), by default the current one minus _stg if any (NOT a WITH-defined alias, because it is always used in another _csv.sql model)
- column_models : (TODO rename def_column_models) used to guide parsing of values from text, and add missing columns as NULL if enabled (complete_columns_with_null)
the first column with a given name DEFINES EXACTLY the type of this column, others are converted to it (so must be compatible obviously).
(TODO Q So only the first column with a given name is kept?)
if none, source is used as single column_models (same as defined_columns_only=false).
- defined_columns_only : if true only produces def models columns, otherwise also all other source columns (which must
not conflict)
- complete_columns_with_null
- wkt_rather_than_geojson
- date_formats : in the order of parsing preference, by default : 'YYYY-MM-DDTHH24:mi:ss.SSS' (RFC3339), 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'
- geo_pattern
- best_geometry_columns : allows to prioritize which column in source is better to be mapped to the single column
typed as geometry in def models  (so also allows to rename / map it)
- target_geometry_column_name : used if no column_models to change the source geometry column name (useful because
a lot of formats ex. geopackage, Shapefile don't have a name for it, so its name is created by the import tool ex. ogr2ogr)
- srid : geo srid
- uuid_pattern
- def_from_source_mapping : allows ex. to add prefixes and rename according to the data dictionary
- data_dict_schema_table : if any, allows to join to it to translated codes accordingly
- data_dict_code_columns : list of def colummn names ; if any, tells on which fields to join to translated codes accordingly
- debug : also adds the source field with the __src suffix
#}


{% macro from_csv(source, column_models=[], defined_columns_only=false, complete_columns_with_null=false,
    date_formats=['YYYY-MM-DDTHH24:mi:ss.SSS', 'YYYY/MM/DD HH24:mi:ss.SSS', 'DD/MM/YYYY HH24:mi:ss.SSS'],
    geo_pattern=".*geo.*", wkt_rather_than_geojson=false, best_geometry_columns=['geom', 'Geom', 'geometrie'],
    target_geometry_column_name='geometry', srid='2154', fdr_src_perimetre_all_parsed_exists=false,
    uuid_pattern="_Id|_Ref", def_from_source_mapping={},
    data_dict_schema_table=None, data_dict_code_columns=[], debug=true) %}

{% set no_column_models = column_models | length == 0 %}
{% set source = source if source else ref(model.name | replace('_stg', '')) %}

{%- set cols = adapter.get_columns_in_relation(source) | list -%}
{%- set col_names = cols | map(attribute='name') | list -%}

{%- set all_col_names = [] -%}
{%- set all_def_cols = [] -%}
{# add columns that are in defs : #}
{% for column_model in column_models %}
  {% for col in adapter.get_columns_in_relation(column_model) | list %}
    {% if col.name not in all_col_names %}
      {% if all_def_cols.append(col) %}{% endif %}
      {% if all_col_names.append(col.name) %}{% endif %}
    {% endif %}
  {% endfor %}
{% endfor %}
{% if not defined_columns_only or no_column_models %}
  {# add columns that are not in defs : #}
  {% for col in cols %}
    {% if col.name not in all_col_names %}
      {% if all_def_cols.append(col) %}{% endif %}
      {% if all_col_names.append(col.name) %}{% endif %}
    {% endif %}
  {% endfor %}
{% endif %}

{% set vars = { "best_geometry_col_name" : None, "chosen_geometry_column_name" : None, } %}
{% if best_geometry_columns %}
  {# find best geometry column : #}
  {% for col_name in best_geometry_columns %}
    {# -- {{ "from_csv ? best_geometry_col " ~  col_name ~ " " ~ fdr_francedatareseau.get_column(cols, col_name) }} #}
    {% if not vars.best_geometry_col_name and fdr_francedatareseau.get_column(cols, col_name).data_type == 'USER-DEFINED' or fdr_francedatareseau.get_column(cols, col_name).data_type == 'text' %}
        {% if vars.update({'best_geometry_col_name': col_name}) %} {% endif %}{# NOT set col_found = col : https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop #}
    {% endif %}
  {% endfor %}
{% endif %}
{% set best_geometry_col_name = vars.best_geometry_col_name %}

{%- set def_cols = all_def_cols if complete_columns_with_null else (all_def_cols | selectattr("name", "in", col_names) | list) -%}

-- from_csv {{ source }} :
-- complete_columns_with_null : {{ complete_columns_with_null }} ; col_names : {{ col_names }}
-- no_column_models : {{ no_column_models }} ; defined_columns_only : {{ defined_columns_only }} ; def_cols : {{ def_cols }}
-- def_from_source_mapping : {{ def_from_source_mapping }}
-- data_dict_schema_table : {{ data_dict_schema_table }} ; data_dict_code_columns : {{ data_dict_code_columns }}
-- cols : {{ cols }}
-- geo_pattern : {{ geo_pattern }} ; best_geometry_columns : {{ best_geometry_columns }} => {{ best_geometry_col_name }} ; target_geometry_column_name : {{ target_geometry_column_name }}
with converted as (
select

    {% for def_col in def_cols %}
        {% set mapped_source_col_name_same_case = def_from_source_mapping[def_col.name] if def_from_source_mapping[def_col.name] and def_from_source_mapping[def_col.name] in col_names else None %}
        {% set mapped_source_col_name_lower_case = def_from_source_mapping[def_col.name].lower() if def_from_source_mapping[def_col.name] and def_from_source_mapping[def_col.name].lower() in col_names else None %}
        {% set mapped_source_col_name = mapped_source_col_name_same_case if mapped_source_col_name_same_case else mapped_source_col_name_lower_case if mapped_source_col_name_lower_case else def_col.name %}
        {% set is_target_geometry_column = modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) and def_col.data_type == 'USER-DEFINED' %}
        {% set source_col_name = best_geometry_col_name if is_target_geometry_column and best_geometry_col_name else mapped_source_col_name %}
        -- source_col_name : {{ source_col_name }} ; mapping : {{ def_from_source_mapping[def_col.name] }} => same_case : {{ mapped_source_col_name_same_case }} ; lower_case : {{ mapped_source_col_name_lower_case }} ; def_col.data_type : {{ def_col.data_type }} ; def_col.name :  {{ def_col.name }} ; is_target_geometry_column : {{ is_target_geometry_column }} ; geo pattern : {{ True if modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) else false }} ; source col : {{ fdr_francedatareseau.get_column(cols, source_col_name) }} ; not in source : {{ source_col_name not in col_names }}

        {# TODO (but not for parsing from table) first of column_models must provide the type and therefore by 0-lined NOO ONLY IN dbt_utils.union()
        (required anyway to define EXACTLY the column type, so better than doing it in parsing macros, or here) #}
        {% if source_col_name not in col_names %}
          NULL::{% if def_col.is_number() %}numeric{#% NOO else eaupotcan_qualiteGeolocalisation ! elif modules.re.match(geo_pattern, def_col.name, modules.re.IGNORECASE) %}geometry#}{% elif def_col.data_type == 'date' or def_col.data_type == 'timestamp' or def_col.data_type == 'timestamp with time zone' %}date{% elif def_col.data_type == 'boolean' %}boolean{% else %}text{% endif %} as {{ adapter.quote(def_col.name) }}
          {# NULL as {{ adapter.quote(def_col.name) }} #}
          , NULL as {{ adapter.quote(def_col.name + '__src') }}
        {% else %}

        {% set source_col = cols | selectattr("name", "eq", source_col_name) | list | first %}

        {% if is_target_geometry_column %}
          {# this is the target geometry column. If best_geometry_col_name, use it as source. #}
          {% if vars.update({'chosen_geometry_column_name': target_geometry_column_name if no_column_models and target_geometry_column_name else def_col.name }) %} {% endif %}
          {{ fdr_francedatareseau.to_geometry_or_null(source_col_name, source, wkt_rather_than_geojson=wkt_rather_than_geojson, srid=srid) }} as {{ adapter.quote(vars.chosen_geometry_column_name) }}
        {# ARRAY : if also in source keep it so, else TODO from json : according to param, example data, meta ? NOO TODO json_to_array
        {% elif def_col.data_type == 'ARRAY' %}
          array_to_json({{ source }}.{{ adapter.quote(source_col.name) }}) as {{ adapter.quote(def_col.name) } #}
        {% elif def_col.data_type == 'ARRAY' and source_col.data_type == 'ARRAY' %}
          {{ source }}.{{ adapter.quote(source_col.name) }}
        {% elif modules.re.match(uuid_pattern, def_col.name) %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::uuid
        {% elif def_col.is_number() %}
          {{ fdr_francedatareseau.to_numeric_or_null(source_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          -- {# "{{ schema }}".fdr_francedatareseau.to_numeric_or_null({{ source }}.{{ adapter.quote(def_col.name) }}) as {{ adapter.quote(def_col.name) }} #} -- or merely ::numeric ?
          --{{ source }}.{{ adapter.quote(def_col.name) }}::numeric -- NOT to_numeric_or_null else No function matches the given name and argument types.
        {% elif def_col.data_type == 'date' or def_col.data_type == 'timestamp' or def_col.data_type == 'timestamp with time zone' %}-- date
          "{{ schema }}".to_date_or_null({{ source }}.{{ adapter.quote(source_col.name) }}::text, {% for fmt in date_formats %}'{{ fmt }}'::text{% if not loop.last %}, {% endif %}{% endfor %}) as {{ adapter.quote(def_col.name) }}
        {% elif def_col.data_type == 'boolean' %}
          {{ fdr_francedatareseau.to_boolean_or_null(source_col.name, source) }} as {{ adapter.quote(def_col.name) }}
          --"{{ schema }}".to_boolean_or_null({{ source }}.{{ adapter.quote(source_col.name) }}) as {{ adapter.quote(def_col.name) }} -- ? allows for 'oui'
        {# % elif def_col.is_string() %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name) }} -- in case it's NOT text ex. int4 because of dbt seed !
        #}
        {% else %}
          {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name) }}
        {% endif %}


        {% if debug %} -- TODO only if not ::text'd
          , {{ source }}.{{ adapter.quote(source_col.name) }}::text as {{ adapter.quote(def_col.name + '__src') }}
        {% endif %}

        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}
    --, '{ "a":1, "b":"zz" }'::json as test

    from {{ source }}

)
select
{% if data_dict_schema_table and defined_columns_only %}
    -- translate codes according to data dictionary :
    {% for def_col in def_cols %}
        {% if modules.re.sub('.*_', '', def_col.name) in data_dict_code_columns %}
            "dict_{{ def_col.name }}"."Code" as {{ adapter.quote(def_col.name) }} -- TODO or keep orig value : coalesce(, x ?)
        {% else %}
            converted.{{ adapter.quote(def_col.name) }}
        {% endif %}
        {% if debug %} -- TODO only if not ::text'd
          , converted.{{ adapter.quote(def_col.name + '__src') }}
        {% endif %}
        {% if not loop.last %},{% endif %}
    {% endfor %}

{% else %}
converted.*
{% endif %}
{# NOO  % if vars.chosen_geometry_column_name %}
, ST_Transform("{{ vars.chosen_geometry_column_name }}", 4326) as geometry_4326,
ST_Transform("{{ vars.chosen_geometry_column_name }}", 2154) as geometry_2154
{% endif %#}
-- TOO LONG 4s => 24s...
-- adding perimeter owner : {{ chosen_geometry_column_name }} {{ fdr_src_perimetre_all_parsed_exists }}
--, {% if vars.chosen_geometry_column_name and fdr_src_perimetre_all_parsed_exists %}p.data_owner_id{% else %}NULL{% endif %} as perimetre_data_owner_id


from converted
--{% if vars.chosen_geometry_column_name and fdr_src_perimetre_all_parsed_exists %}
--, "france-data-reseau".fdr_src_perimetre_all_parsed p
--where ST_Contains(p.geom, {{ adapter.quote(vars.chosen_geometry_column_name) }})

{% if data_dict_schema_table %}
    -- joins to translate codes according to data dictionary :
    {% for def_col in def_cols %}
        {% if modules.re.sub('.*_', '', def_col.name) in data_dict_code_columns %}
            left join {{ data_dict_schema_table }} "dict_{{ def_col.name }}"
                on converted."{{ def_col.name }}" = "dict_{{ def_col.name }}"."Valeur"
                -- TODO TODO and "dict_{{ def_col.name }}"."Champs" = '{{ def_col.name | replace('eaupotcan_', '') }}'
                and "dict_{{ def_col.name }}"."Valeur" is not null and "dict_{{ def_col.name }}"."Code" is not null -- ?
        {% endif %}
    {% endfor %}
{% endif %}

{% endif %}

{% endmacro %}


{% macro get_column(cols, col_name) %}
{% set vars = { 'col_found' : None } %}
{% for col in cols %}
    {#% do log("get_column ? col " ~  col_name ~ " " ~ col ~ " " ~ (col.name == col_name) ~ " " ~ (not col_found), info=True) %#}
    {% if not vars.col_found and col.name == col_name %}
        {% if vars.update({'col_found': col}) %} {% endif %}{# NOT set col_found = col : https://stackoverflow.com/questions/9486393/jinja2-change-the-value-of-a-variable-inside-a-loop #}
        {#% do log("get_column set : col_found " ~  col_found, info=True) %#}
    {% endif %}
{% endfor %}
{#% do log("get_column end : col_found " ~  col_found, info=True) %#}
{{ return(vars.col_found) }}
{% endmacro %}