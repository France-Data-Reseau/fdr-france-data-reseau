{#
Macros that converts and does a union view of different source imported tables according to FDR_ custom fields metadata
(especially FDR_USE_CASE, FDR_SOURCE_NOM).
TODO FDR_ROLE=source
#}


{#
Creates all possible union views of conversions of different source imported tables according to FDR_ custom fields metadata
(especially FDR_USE_CASE, FDR_SOURCE_NOM), by calling fdr_source_union_from_import_row().
Not really useful because forces no def_model, target_geometry_column_name ('geometry'), srid (2154) ;
TODO LATER does not full work anymore in all cases because of new parameters of fdr_source_union_from_import_row().
TODO LATER get the first one by naming convention from FDR_SOURCE_NOM, and the last ones by conf per FDR_USE_CASE.
#}
{% macro create_views_fdr_source_unions() %}
{{ log("create_views_union start", info=True) }}
{% set fdr_import_resource_model = source("fdr_import", "fdr_import_resource")  %}

{% set fdr_sources = fdr_sources(sql, fdr_import_resource_model) %}
{% do log("create_views_union res " ~ fdr_sources, info=True) %}{# see https://docs.getdbt.com/reference/dbt-jinja-functions/run_query https://agate.readthedocs.io/en/latest/api/table.html #}

{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}
{% for source_row in fdr_sources.rows %}
    {{ log("create_view_union start... ", info=True) }}
    {% set sql %}
    BEGIN;
    create view {{ source_row['schema'] }}."{{ source_row['use_case_prefix'] ~ '_raw_' ~ source_row['FDR_SOURCE_NOM'] }}" as
    {{ fdr_source_union_from_import_row(source_row, fdr_import_resource_model) }}
    ;
    COMMIT; -- else does not create view ! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
    {% endset %}
    {% do log("create_views_union source_row sql " ~ sql, info=True) %}
    {% do run_query(sql) %}
    {% do log("create_views_union source_row done", info=True) %}
{% endfor %}
{% endif %}
{{ log("create_views_union end") }}
{% endmacro %}


{#
Used by fdr_source_union_from_name(). Returns ARRAYs of matching tables with the same "schema", "FDR_SOURCE_NOM",
has_dictionnaire_champs_valeurs in the fdr_import_resource relation (that is the output of import.py).
Groups them by FDR_CAS_USAGE and data_owner_id, because that's the perimeter of FDR_SOURCE_NOM (and of a data
dictionary).
- forced_source_nom : use it if the grouped tables don't all have the same FDR_SOURCE_NOM,
to provide the middle part of the union view that will be created
- forced_use_case_prefix : ONLY IF USED FROM the global create_views_fdr_source_unions() : use it if the grouped tables
don't all have the same use_case_prefix (so typically "fdr"), to provide the prefix of the table that will be created
#}
{% macro fdr_sources(fdr_source_criteria,
    forced_source_nom=None,
    forced_use_case_prefix=None,
    fdr_import_resource_model=source("fdr_import", "fdr_import_resource")) %}
{% set sql %}

select
    {% if forced_source_nom %}'{{ forced_source_nom }}'{% else %}s."FDR_SOURCE_NOM"{% endif %} as "FDR_SOURCE_NOM",
    {% if forced_use_case_prefix %}'{{ forced_use_case_prefix }}'{% else %}min(s."use_case_prefix"){% endif %} as "use_case_prefix",
    json_agg(json_build_object(
    	'schema', s."schema", 'table', s."table", 'FDR_SOURCE_NOM', s."FDR_SOURCE_NOM",
    	'data_owner_dict_schema', data_owner_dict."schema", 'data_owner_dict_table', data_owner_dict."table"
    )) as cas_usage_source_tables,
    s."FDR_CAS_USAGE" as "FDR_CAS_USAGE"
    --, s."FDR_SOURCE_NOM" as "FDR_SOURCE_NOM",
    --min(s."use_case_prefix") as "use_case_prefix"
from "france-data-reseau".fdr_import_resource s
left join ( -- LEFT join so that there is a line even if there is no data dict
select
    data_owner_id as data_dict_data_owner_id, "FDR_CAS_USAGE" as "data_dict_FDR_CAS_USAGE",
    -- TODO "FDR_SOURCE_NOM",
    min("schema") as "schema", min("table") as "table"
from "france-data-reseau".fdr_import_resource
where status = 'success' and "FDR_TARGET" <> 'archive'
and ("FDR_SOURCE_NOM" = 'dictionnaire_champs_valeurs_raw' or "FDR_SOURCE_NOM" like '%_dict') -- TODO ..._dict case
group by "FDR_CAS_USAGE", data_owner_id

) data_owner_dict on s."FDR_CAS_USAGE" = data_owner_dict."data_dict_FDR_CAS_USAGE" and s.data_owner_id = data_owner_dict.data_dict_data_owner_id
where status = 'success' and "FDR_TARGET" <> 'archive' -- better than no errors or "FDR_SOURCE_NOM" is not null
{% if fdr_source_criteria %}
and {{ fdr_source_criteria }}
{% endif %}
--group by data_owner_dict.data_owner_dict_schema, data_owner_dict.data_owner_dict_table;

group by s."FDR_CAS_USAGE", s."FDR_SOURCE_NOM";
{% endset %}
{% do log("fdr_sources sql " ~ sql, info=True) %}
{% set fdr_sources = run_query(sql) %}
{% do log("fdr_sources res " ~ fdr_sources, info=True) %}{# see https://docs.getdbt.com/reference/dbt-jinja-functions/run_query https://agate.readthedocs.io/en/latest/api/table.html #}
{{ return(fdr_sources) }}
{% endmacro %}


{#
Creates the union view of conversions of different source imported tables according to FDR_ custom fields metadata
(especially FDR_USE_CASE, FDR_SOURCE_NOM), by calling fdr_source_union_from_import_row().
params :
- FDR_SOURCE_NOM, FDR_CAS_USAGE, has_dictionnaire_champs_valeurs to build the filter criteria, see how fdr_sources()
uses them
- see fdr_source_union_from_import_row()
#}
{% macro fdr_source_union_from_name(FDR_SOURCE_NOM, has_dictionnaire_champs_valeurs, context_model,
        def_model=None,
        best_geometry_columns=['geom', 'Geom', 'geometrie'], target_geometry_column_name='geometry', srid='2154',
        def_from_source_mapping = None,
        FDR_CAS_USAGE=var('FDR_CAS_USAGE')) %}
{% set sql_criteria %}
"FDR_CAS_USAGE" = '{{ FDR_CAS_USAGE }}' and "FDR_SOURCE_NOM" = '{{ FDR_SOURCE_NOM }}'
{% endset %}
{{ fdr_francedatareseau.fdr_source_union_from_criteria(sql_criteria, has_dictionnaire_champs_valeurs, context_model,
       def_model=def_model,
       best_geometry_columns=best_geometry_columns, target_geometry_column_name=target_geometry_column_name, srid=srid,
       def_from_source_mapping = def_from_source_mapping) }}
{% endmacro %}

{#
See fdr_source_union_from_name() and fdr_sources()
#}
{% macro fdr_source_union_from_criteria(source_sql_criteria, has_dictionnaire_champs_valeurs, context_model,
        def_model=None,
        forced_source_nom=None, forced_use_case_prefix=None,
        best_geometry_columns=['geom', 'Geom', 'geometrie'], target_geometry_column_name='geometry', srid='2154',
        def_from_source_mapping = None) %}
{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}
{% set source_rows = fdr_francedatareseau.fdr_sources(source_sql_criteria,
    forced_source_nom=forced_source_nom, forced_use_case_prefix=forced_use_case_prefix) %}
{% if source_rows.rows | length == 0 %}
  {# { exceptions.raise_compiler_error("fdr_source_union ERROR : no table to be unioned found, check parameters of fdr_source_union_from_name()") } #}
  {% do log("fdr_source_union WARNING : no table to be unioned found, maybe check parameters of fdr_source_union_from_name()", info=True) %}
  {% set source_row = None %}
{% else %}
    {% set source_row = source_rows.rows[0] %}
{% endif %}

{{ fdr_francedatareseau.fdr_source_union_from_import_row(source_row, context_model, def_model,
    best_geometry_columns=best_geometry_columns, target_geometry_column_name=target_geometry_column_name, srid=srid,
    def_from_source_mapping=def_from_source_mapping) }}
{% endif %}
{% endmacro %}


{#
Creates the union view of conversions of different source imported tables provided in source_row.
Finds the data dictionary if any corresponding to use case (FDR_CAS_USAGE) and collectivité / data provider (data_owner_id),
gets from it the column mapping (and pass it to the builder of def_from_source_mapping, which therefore must not be provided)
and the code column names (and pass it to from_csv() to build the join-based translation).
Conversion is done by from_csv() (see doc there) using the provided parameters.
- source_row : None is accepted, if def_model is provided so as to be able to still generate a table rather than explode
- context_model
- def_model : DEFINES EXACTLY the target column types ; if None all found columns are unioned and must have the same type
- others : see use in from_csv()
NB. any specific translated_macro must rather be applied after calling this macro.
#}
{% macro fdr_source_union_from_import_row(source_row, context_model, def_model=None,
        best_geometry_columns=['geom', 'Geom', 'geometrie'], target_geometry_column_name='geometry', srid='2154',
        def_from_source_mapping = None) %}
{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}

{% if source_row %}

-- get all matching relation names :
{% do log("fdr_source_union start " ~ source_row ~ context_model.database, info=True) %}
{% set cas_usage_source_tables = fromjson(source_row['cas_usage_source_tables']) %}
{% do log("fdr_source_union cas_usage_source_tables " ~ cas_usage_source_tables, info=True) %}
{% for cas_usage_source_table in cas_usage_source_tables %}
    {% do log("fdr_source_union table " ~ cas_usage_source_table ~ " found : " ~ adapter.get_relation(database = context_model.database,
        schema = cas_usage_source_table.schema, identifier = cas_usage_source_table.table), info=True) %}
    {% set source_model = adapter.get_relation(database = context_model.database,
        schema = cas_usage_source_table.schema, identifier = cas_usage_source_table.table) %}
    {% if source_model %}
        {% do cas_usage_source_table.update({ 'source_model' : source_model }) %}
    {% else %}
    {% do log("fdr_source_union can't find relation ! " ~ cas_usage_source_table) %}
    {% endif %}

    -- get 2 confs from dict if any :
    {% if cas_usage_source_table.data_owner_dict_table %}
        {% set data_dict_column_mapping_rows = run_query('select jsonb_object_agg("Champs", to_jsonb(t) - \'Champs\') res from "'
            ~ cas_usage_source_table.data_owner_dict_schema ~ '"."' ~ cas_usage_source_table.data_owner_dict_table ~ '"'
            ~ ' t where t."Valeur" is not null and t."Code" is null and t."FDR_SOURCE_NOM"='
            ~ "'" ~ cas_usage_source_table.FDR_SOURCE_NOM ~ "'") %}
        {% if data_dict_column_mapping_rows and data_dict_column_mapping_rows | length != 0 %}
            {% do cas_usage_source_table.update({ 'data_dict_column_mappings' : fromjson(data_dict_column_mapping_rows[0]['res']) }) %}
        {% endif %}
        {% set data_dict_code_columns_rows = run_query('select json_agg(distinct "Champs") res from "'
            ~ cas_usage_source_table.data_owner_dict_schema ~ '"."' ~ cas_usage_source_table.data_owner_dict_table ~ '"'
            ~ ' t where t."Code" is not null and t."Valeur" is not null') %}
        {# TODO ' and t."FDR_SOURCE_NOM"=' ~ "'" ~ cas_usage_source_table.FDR_SOURCE_NOM ~ "'" #}
        {% if data_dict_code_columns_rows and data_dict_code_columns_rows | length != 0 %}
            {% do cas_usage_source_table.update({ 'data_dict_code_columns' : fromjson(data_dict_code_columns_rows[0]['res']) }) %}
        {% endif %}
        {% do log("cas_usage_source_table updated " ~ cas_usage_source_table, info=True) %}
    {% endif %}
{% endfor %}
{% do log("fdr_source_union source_models " ~ source_models, info=True) %}

{% set sql %}
--create view {{ source_row['schema'] }}."{{ source_row['use_case_prefix'] ~ '_raw_' ~ source_row['FDR_SOURCE_NOM'] }}" as
-- no, rather in DBT models (so no need for use_case_prefix & FDR_SOURCE_NOM - unless from global create_views_fdr_source_unions())

{% set fdr_src_perimetre_all_parsed_exists = adapter.get_relation(database = context_model.database,
    schema = 'france-data-reseau', identifier = 'fdr_src_perimetre_all_parsed') %}
{% do log("fdr_source_union def_from_source_mapping " ~ def_from_source_mapping
    ~ " fdr_src_perimetre_all_parsed_exists " ~ ('yes' if fdr_src_perimetre_all_parsed_exists else 'no'), info=True) %}
{% set defined_columns_only = not not def_model %}
{% for cas_usage_source_table in cas_usage_source_tables %}
    -- build mapping, using data dict conf if any :
    {% set def_from_source_mapping = fdr_francedatareseau.build_def_from_source_mapping(def_model,
        data_dict_column_mappings=cas_usage_source_table.data_dict_column_mappings if data_dict_column_mappings in cas_usage_source_table else None) %}
    (
    with lenient_parsed as (
    {{ fdr_francedatareseau.from_csv(cas_usage_source_table.source_model, column_models=[def_model] if def_model else [],
        defined_columns_only=defined_columns_only, complete_columns_with_null=true,
        wkt_rather_than_geojson=true, best_geometry_columns=best_geometry_columns,
        fdr_src_perimetre_all_parsed_exists=fdr_src_perimetre_all_parsed_exists,
        target_geometry_column_name=target_geometry_column_name, srid=srid,
        def_from_source_mapping=def_from_source_mapping,
        data_dict_schema_table='"' ~ cas_usage_source_table.data_owner_dict_schema  ~ '"."' ~ cas_usage_source_table.data_owner_dict_table ~ '"',
        data_dict_code_columns=cas_usage_source_table.data_dict_code_columns) }}
    --limit 5 -- TODO better
    )
    -- select '"{{ context_model.database }}"."{{ cas_usage_source_tables.schema }}"."{{ cas_usage_source_table.source_model }}"'::text as src_relation, lenient_parsed.* from lenient_parsed
    select '{{ source_model }}'::text as import_table, lenient_parsed.*,
        s.last_changed, s."data_owner_id", s.org_name as data_owner_label, -- TODO org_title
        s."FDR_CAS_USAGE", s."FDR_ROLE", s."FDR_SOURCE_NOM", s."FDR_TARGET"
    from lenient_parsed
        left join "france-data-reseau".fdr_import_resource s
        on '{{ cas_usage_source_table.source_model }}'::text = '"{{ target.database }}"."{{ cas_usage_source_table.schema }}"."' || s."table" || '"' -- "datastore"."eaupotable"."eaupot_raw_dictionnaire_champs_valeurs_93edagglo"
    where s.status = 'success' -- ?
    )
    {% if not loop.last %}
    UNION ALL -- without ALL removes duplicates lines according to the columns of the first column statement i.e. import_table so all save one !
    {% endif %}
{% endfor %}

{% endset %}

{% else %}
{# no source_row, let's compensate by generating an empty table : #}
{% do log("WARNING fdr_source_union_from_import_row  : no source row found matching given criteria (see SQL above).", info=True) %}
{% if not def_model %}
    {{ exceptions.raise_compiler_error("ERROR fdr_source_union_from_import_row : no table to be unioned found, and no def_model so can't generate empty table instead") }}
{% endif %}
{% set sql %}
select '' as import_table, *,
    NULL as last_changed, NULL as "data_owner_id", NULL as data_owner_label, -- TODO org_title
    NULL as "FDR_CAS_USAGE", NULL as "FDR_ROLE", NULL as "FDR_SOURCE_NOM", NULL as "FDR_TARGET"
from {{ def_model }}
{% endset %}
{% endif %}

{{ sql }}
-- TODO below is only when used from global create_views_fdr_source_unions(), otherwise it's the above inlining !
{#% do log("create_view_union sql " ~ sql, info=True) %}
{% do run_query(sql) %}
{% do log("create_view_union done", info=True) %#}

{% endif %}
{% endmacro %}


{#
Used by from_csv().
Returns a dict providing for each target column, the name where to look it up to in the source :
- (unless disabled) without the front use case prefix (maps from 'eaupot.*_')
- (unless disabled) without the first comma (maps ex. "pt_code,C,254" from pt_code)
- NB. NOT lower case field (because ogr2ogr does it...), rather tried out in from_csv()
- get the corresponding mapped name from data_dict_column_mappings if any
#}
{% macro build_def_from_source_mapping(def_column_model, data_dict_column_mappings={},
    remove_prefix=True, remove_after_comma=True) %}
{% set def_from_source_mapping = {} %}
{% if def_column_model %}
    {% for col in adapter.get_columns_in_relation(def_column_model) | list %}
      {% set without_prefix = modules.re.sub('.*_', '', col.name) if remove_prefix else col.name %}
      {% set without_after_comma = modules.re.sub(',.*', '', without_prefix) if remove_after_comma else without_prefix %}
      {% set source_mapping = data_dict_column_mappings[without_after_comma] %}
      {% if def_from_source_mapping.update({ col.name : without_after_comma if not source_mapping else source_mapping.Valeur }) %}{% endif %}
    {% endfor %}
{% endif %}{# else return empty, so from_csv will use unmapped column name #}
{{ return(def_from_source_mapping) }}
{% endmacro %}



