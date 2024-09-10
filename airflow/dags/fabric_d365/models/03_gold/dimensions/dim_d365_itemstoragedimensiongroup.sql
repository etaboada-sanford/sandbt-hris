{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_itemstoragedimensiongroup_sk']
) }}

select
    sdg.[Id] as dim_d365_itemstoragedimensiongroup_sk
    , sdg.recid as itemstoragedimensiongroup_recid
    , sdg.name as itemstoragedimensiongroupid
    , sdg.description as itemstoragedimensiongroup_name
    , sdg.[IsDelete]
    , sdg.versionnumber
    , sdg.sysrowversion
from {{ source('fno', 'ecoresstoragedimensiongroup') }} as sdg
{%- if is_incremental() %}
    where sdg.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where sdg.[IsDelete] is null
{% endif %}