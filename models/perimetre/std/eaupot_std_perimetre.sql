{#

#}

{{
  config(
    materialized="incremental",
    unique_key=['"FDR_CAS_USAGE"', 'data_owner_id'],
    tags=['incremental'],
  )
}}

{% set sourceModel = ref(this.name | replace('_std_', '_src_') ~ '_parsed') %}

select * from {{ sourceModel }}

{% if is_incremental() %}
  where last_changed > (select coalesce(max(last_changed), '1970-01-01T00:00:00') from {{ this }})
{% endif %}