{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_itemgroup_sk']
) }}

select
    iig.[Id] as dim_d365_itemgroup_sk
    , iig.recid as itemgroup_recid
    , iig.itemgroupid
    , iig.name as itemgroup_name
    , iig.dxc_israwmaterial
    , iig.partition
    , iig.[IsDelete]
    , upper(iig.dataareaid) as itemgroup_dataareaid
    , iig.versionnumber
    , iig.sysrowversion
from {{ source('fno', 'inventitemgroup') }} as iig
{%- if is_incremental() %}
    where iig.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where iig.[IsDelete] is null
{% endif %}