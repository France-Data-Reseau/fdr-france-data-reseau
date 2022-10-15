{#
2 phase n-n reconciliation / linking - phase 1 produce linked object ids from subject,
supports DBT incremental (filtered on translated_source last_changed whose max is added as a dummy marker line
- but not on commune so if they change do a --full-refresh, use it with
unique_key=['"' + fieldPrefix + "IdSupportAerien" + '"', 'com_code'],)

Produces the table (materialized because computes) of the n-n relationship between a type and commune
Used in apcom/eaupot _commune_linked
120s on apcomsup _unified TODO using no index
#}

{% macro fdr_2phase1link_commune_geometry(translated_source, id_field, geometry_field, fields, order_by=None, srid="4326") %}

{% set com_geometry_column = "geometry_shape_2154" if srid == "2154" else "geometry" %}

with link_candidates as (
    -- 54s on 1m osmposup
    select
        {{ translated_source }}."{{ id_field }}", -- * not possible for group by below
        com.com_code,-- as "fdrcommune__insee_id",
        -- other small useful fields to avoid joining to commune most of the times :
        com.com_name,-- as "fdrcommune__name",
        com.epci_code,
        com.epci_name,
        com.dep_code,
        com.dep_name,
        com.reg_code,-- as "fdregion__insee_id",
        com.reg_name -- as "fdregion__name"
        -- TODO population ?

        , {{ translated_source }}.last_changed as translated_last_changed

    FROM {{ translated_source }}, {{ source('france-data-reseau', 'fdr_std_communes_ods') }} com
    --WHERE ST_Contains(ST_GeometryFromText(ST_AsText(c.geo_shape), 4326), {{ translated_source }}.geometry) and c.com_code is not null -- TODO patch source "{{ geometry_field }}" to 4326 SRID
    WHERE ST_Contains(com."{{ com_geometry_column }}", {{ translated_source }}."{{ geometry_field }}") -- and com.com_code is not null -- NOO not needed and bad perfs ; OLD ! removes communes of Nouvelle Calédonie etc.

    --and 1 = (1+1) -- test incremental without table already existing
    {% if is_incremental() %}
      --and {{ translated_source }}.last_changed <= '2022-09-30T15:30:28' -- test incremental in the middle, or change the later_last_changed column
      and {{ translated_source }}.last_changed > (select coalesce(max(max_translated.translated_last_changed), '1970-01-01T00:00:00') from {{ this }} max_translated where max_translated."com_name" is NULL)
    {% endif %}

    --having count(*) > 1 -- TODO idea : store only rare duplicates
    {% if order_by %})
      order by {{ order_by }} -- "{{ id_field }}", com.com_code
    {% endif %}
)

select * from link_candidates

UNION ALL
    select
        'INCREMENTAL_MARKER_DUMMY' as "{{ id_field }}", -- * not possible for group by below
        'INCREMENTAL_MARKER_DUMMY' as com_code,-- as "fdrcommune__insee_id",
        -- other small useful fields to avoid joining to commune most of the times :
        NULL as com_name,-- as "fdrcommune__name",
        NULL as epci_code,
        NULL as epci_name,
        NULL as dep_code,
        NULL as dep_name,
        NULL as reg_code,-- as "fdregion__insee_id",
        NULL as reg_name, -- as "fdregion__name"

        (select max(last_changed) from {{ translated_source }}) as translated_last_changed
          
{% endmacro %}