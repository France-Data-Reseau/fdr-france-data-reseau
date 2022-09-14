{#

rename and generic parsing is rather done
- in specific _from_csv
- in generic from_csv (called by fdr_source_union), which is guided by the previous one
#}

{% macro add_generic_fields(specific_parsed_alias, fieldPrefix, ns, src_priority=None) %}

with src_renamed as (

    select
        *,

        --'{{ parsed_source_relation }}' as "{{ fieldPrefix }}src_name", -- source name, for src_id (with data_owner_id) and _priority (else won't have it anymore once unified with other sources)
        "FDR_SOURCE_NOM" as "{{ fieldPrefix }}src_kind", -- source kind / type, for src_id (with data_owner_id) and _priority (else won't have it anymore once unified with other sources)
        "FDR_SOURCE_NOM" || '_' || data_owner_id as "{{ fieldPrefix }}src_name", -- source name, for src_id (else won't have it anymore once unified with other sources)
        import_table as "{{ fieldPrefix }}src_table" -- (bonus)
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