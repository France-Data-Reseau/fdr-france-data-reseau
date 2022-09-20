{#
(rather than as dedicated dbt models ? alas in both cases source requires yaml def...)
il manquait BEGIN; COMMIT; pour que create view dans macro marche !! https://docs.getdbt.com/reference/dbt-jinja-functions/run_query
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


{% macro fdr_sources(fdr_source_criteria, fdr_import_resource_model=source("fdr_import", "fdr_import_resource")) %}
{% set sql %}
select s."schema", s."FDR_SOURCE_NOM", data_owner_dict.has_dictionnaire_champs_valeurs,
min(s.use_case_prefix) as use_case_prefix, ARRAY_AGG("table")as tables
from "france-data-reseau".fdr_import_resource s left join (
select sd.data_owner_id, case sd.data_owner_id when null then false else true end as has_dictionnaire_champs_valeurs
from "france-data-reseau".fdr_import_resource sd
where sd."FDR_SOURCE_NOM" = 'dictionnaire_champs_valeurs'
group by sd.data_owner_id
) data_owner_dict on s.data_owner_id = data_owner_dict.data_owner_id
--from {{ fdr_import_resource_model }} s -- from eaupot outputs : "datastore"."eaupotable"."eaupot_src_canalisations_en_service" s !?!
where status = 'success' and "FDR_TARGET" <> 'archive' -- better than no errors or "FDR_SOURCE_NOM" is not null
{% if fdr_source_criteria %}
and {{ fdr_source_criteria }}
{% endif %}
group by "schema", "FDR_SOURCE_NOM", data_owner_dict.has_dictionnaire_champs_valeurs;
{% endset %}
{% do log("fdr_sources sql " ~ sql, info=True) %}
{% set fdr_sources = run_query(sql) %}
{% do log("fdr_sources res " ~ fdr_sources, info=True) %}{# see https://docs.getdbt.com/reference/dbt-jinja-functions/run_query https://agate.readthedocs.io/en/latest/api/table.html #}
{{ return(fdr_sources) }}
{% endmacro %}


{#
params : see fdr_source_union
TODO separate fdr_source_union_from_import_criteria ?
#}
{% macro fdr_source_union_from_name(FDR_SOURCE_NOM, has_dictionnaire_champs_valeurs, context_model, translated_macro=None, def_model=None, def_from_source_mapping = fdr_francedatareseau.build_def_from_source_mapping_noprefix_lower(def_model), FDR_CAS_USAGE=var('FDR_CAS_USAGE')) %}
{% set sql_criteria %}
"FDR_CAS_USAGE" = '{{ FDR_CAS_USAGE }}' and "FDR_SOURCE_NOM" = '{{ FDR_SOURCE_NOM }}'
and {{ '' if has_dictionnaire_champs_valeurs else 'not' }} has_dictionnaire_champs_valeurs is true
{% endset %}

{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}
{% set source_rows = fdr_francedatareseau.fdr_sources(sql_criteria) %}
{% if source_rows.rows | length == 0 %}
  {# { exceptions.raise_compiler_error("fdr_source_union ERROR : no table to be unioned found, check parameters of fdr_source_union_from_name()") } #}
  {% do log("fdr_source_union WARNING : no table to be unioned found, maybe check parameters of fdr_source_union_from_name()", info=True) %}
  {% set source_row = None %}
{% else %}
    {% set source_row = source_rows.rows[0] %}
{% endif %}

{{ fdr_francedatareseau.fdr_source_union_from_import_row(source_row, context_model, def_model, def_from_source_mapping) }}
{% endif %}
{% endmacro %}


{#
- source_row : None is accepted, if def_model is provided so as to be able to still generate a table rather than explode
- context_model
- def_model : DEFINES EXACTLY the target column types ; if None all found columns are unioned and must have the same type
- def_from_source_mapping : required for translated_macro case
NB. any specific translated_macro must rather be applied after calling this macro.
#}
{% macro fdr_source_union_from_import_row(source_row, context_model, def_model=None, def_from_source_mapping = fdr_francedatareseau.build_def_from_source_mapping_noprefix_lower(def_model)) %}
{% if execute %} {# else Compilation Error 'None' has no attribute 'table' https://docs.getdbt.com/reference/dbt-jinja-functions/execute #}

{% if source_row %}

{% do log("fdr_source_union start " ~ source_row ~ source_row['schema'] ~ context_model.database, info=True) %}
{% set tables = source_row['tables'][2:-2].split('", "') %}{# it's not an array but a string... #}
{% do log("fdr_source_union tables " ~ tables, info=True) %}
{% set source_models = [def_model] if def_model else [] %}
{% for table in tables %}
    {% do log("fdr_source_union table " ~ table ~ " found : " ~ adapter.get_relation(database = context_model.database, schema = source_row['schema'], identifier = table), info=True) %}
    {% set source_model = adapter.get_relation(database = context_model.database, schema = source_row['schema'], identifier = table) %}
    {% if source_model %}
        {% do source_models.append(source_model) %}
    {% else %}
    {% do log("fdr_source_union can't find relation ! " ~ schema ~ '.' ~ table) %}
    {% endif %}
{% endfor %}
{% do log("fdr_source_union source_models " ~ source_models, info=True) %}

{% set sql2 %}

{% do log("fdr_source_union def_from_source_mapping " ~ def_from_source_mapping, info=True) %}
{% set defined_columns_only = not not def_model %}
{% for source_model in source_models %}
    (
    with lenient_parsed as (
    {{ fdr_francedatareseau.from_csv(source_model, column_models=[def_model] if def_model else [source_model], defined_columns_only=defined_columns_only, complete_columns_with_null=true,
        wkt_rather_than_geojson=true, def_from_source_mapping=def_from_source_mapping) }}
    --limit 5 -- TODO better
    )
    -- select '"{{ context_model.database }}"."{{ schema }}"."{{ source_model }}"'::text as src_relation, lenient_parsed.* from lenient_parsed
    select '{{ source_model }}'::text as import_table, lenient_parsed.* from lenient_parsed
    )
    {% if not loop.last %}
    UNION
    {% endif %}
{% endfor %}

{% endset %}

{% else %}
{# no source_row, let's compensate by generating an empty table : #}
{% do log("WARNING fdr_source_union_from_import_row  : no source row found matching given criteria (see SQL above).", info=True) %}
{% if not def_model %}
    {{ exceptions.raise_compiler_error("ERROR fdr_source_union_from_import_row : no table to be unioned found, and no def_model so can't generate empty table instead") }}
{% endif %}
{% set sql2 %}
select '' as import_table, * from {{ def_model }}
{% endset %}
{% endif %}

{% set sql1 %}
--create view {{ source_row['schema'] }}."{{ source_row['use_case_prefix'] ~ '_raw_' ~ source_row['FDR_SOURCE_NOM'] }}" as
with unioned as (
{{ sql2 }}
), enriched as (
-- TODO move to macro used in specific .sql
select
    u.*, s.last_changed, s."data_owner_id", s.org_name as data_owner_label, -- TODO org_title
    s."FDR_CAS_USAGE", s."FDR_ROLE", s."FDR_SOURCE_NOM", s."FDR_TARGET"
from unioned u left join "france-data-reseau".fdr_import_resource s
on u.import_table = '"{{ target.database }}"."{{ source_row['schema'] }}"."' || s."table" || '"' -- "datastore"."eaupotable"."eaupot_raw_dictionnaire_champs_valeurs_93edagglo"
where s.status = 'success'
)
select * from enriched
{% endset %}

{{ sql1 }}
{#% do log("create_view_union sql " ~ sql1, info=True) %}
{% do run_query(sql1) %}
{% do log("create_view_union done", info=True) %#}

{% endif %}
{% endmacro %}


{#
removes the front use case prefix (maps from 'eaupot.*_')
#}
{% macro build_def_from_source_mapping_noprefix_lower(def_column_model) %}
{% set def_from_source_mapping = {} %}
{% if def_column_model %}
    {% for col in adapter.get_columns_in_relation(def_column_model) | list %}
      {% if def_from_source_mapping.update({ col.name : modules.re.sub('.*_', '', col.name.lower()) }) %}{% endif %}
    {% endfor %}
{% endif %}{# else return empty, so from_csv will use unmapped column name #}
{{ return(def_from_source_mapping) }}
{% endmacro %}



