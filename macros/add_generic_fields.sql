{#
Lists import fields (from fdr_resource_import) added by the source union macros().
Useful when SELECTing all fields from several relations that all have these import fields ;
in this case, keep them on the deepest / most specific relation (and elsewhere "except" them using dbt_utils.star())
*ed fields are timestamp
#}
{% macro list_import_fields() %}
{{ return(["import_table", "last_changed", "added", "removed", "imported", "data_owner_id", "data_owner_label", "FDR_CAS_USAGE", "FDR_ROLE", "FDR_SOURCE_NOM", "FDR_TARGET"]) }}
{% endmacro %}

{#
Lists generic fields added by the _translated step. All are computed except from _src_id (which has to be provided).
Useful to remove them (using dbt_utils.star()'s except parameter) from a source in the native / "echange" format
that may already have them but that we want to recompute.
#}
{% macro list_generic_fields(field_prefix) %}
{{ return([field_prefix ~ "src_id", field_prefix ~ "src_kind", field_prefix ~ "src_name", field_prefix ~ "src_table", field_prefix ~ "src_priority", field_prefix ~ "id", field_prefix ~ "uuid"]) }}
{% endmacro %}


{#

rename and generic parsing is rather done
- in specific _from_csv
- in generic from_csv (called by fdr_source_union), which is guided by the previous one

requires :
- "{{ fieldPrefix }}src_id" to exist in specific_parsed_alias
- probably should also require data_owner_id if not FDR_SIREN of the uploader...
#}

{% macro add_generic_fields(specific_parsed_alias, fieldPrefix, ns, src_priority=None) %}

with src_renamed as (

    select
        *,

        --'{{ parsed_source_relation }}' as "{{ fieldPrefix }}src_name", -- source name, for src_id (with data_owner_id) and _priority (else won't have it anymore once unified with other sources)
        "FDR_SOURCE_NOM" as "{{ fieldPrefix }}src_kind", -- source kind / type, for src_id (with data_owner_id) and _priority (else won't have it anymore once unified with other sources)
        --
        "FDR_SOURCE_NOM" || '_' || data_owner_id as "{{ fieldPrefix }}src_name", -- source name, for src_id (else won't have it anymore once unified with other sources)
        import_table as "{{ fieldPrefix }}src_table" -- (bonus) TODO rm
        --id as "{{ fieldPrefix }}src_index", -- index in source

    from {{ specific_parsed_alias }}

), src_computed as (

    select
        *,

        {% if src_priority %}'{{ src_priority }}_' || {% endif %}"{{ fieldPrefix }}src_name" as "{{ fieldPrefix }}src_priority",  -- 0 is highest, then 10, 100, 1000... src_name added to differenciate
        "{{ fieldPrefix }}src_name" || '_' || "{{ fieldPrefix }}src_id" as "{{ fieldPrefix }}id" -- overall unique id

    from src_renamed

), uuid_computed as (

    select
        *,

        uuid_generate_v5(uuid_generate_v5(uuid_ns_dns(), '{{ ns }}'), "{{ fieldPrefix }}id") as "{{ fieldPrefix }}uuid" -- in case of uuid

    from src_computed

)

select * from uuid_computed

{% endmacro %}