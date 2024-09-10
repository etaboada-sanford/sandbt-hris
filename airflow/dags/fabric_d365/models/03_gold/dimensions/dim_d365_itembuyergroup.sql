{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_itembuyergroup_sk']
) }}

select
    ibg.[Id] as dim_d365_itembuyergroup_sk
    , ibg.recid as itembuyergroup_recid
    , ibg.[group] as itembuyergroupid
    , ibg.description as itembuyergroup_name
    , ibg.partition
    , ibg.[IsDelete]
    , ibg.versionnumber
    , ibg.sysrowversion
    , upper(ibg.dataareaid) as itembuyergroup_dataareaid
from {{ source('fno', 'inventbuyergroup') }} as ibg
{%- if is_incremental() %}
    where ibg.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where ibg.[IsDelete] is null
{% endif %}