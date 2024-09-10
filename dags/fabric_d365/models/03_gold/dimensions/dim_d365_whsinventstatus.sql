{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_whsinventstatus_sk']
) }}

select
    whs.[Id] as dim_d365_whsinventstatus_sk
    , whs.recid as whsinventstatus_recid
    , whs.inventstatusid
    , whs.name
    , whs.inventstatusblocking
    , whs.partition
    , whs.[IsDelete]
    , whs.versionnumber
    , whs.sysrowversion
    , upper(whs.dataareaid) as whsinventstatus_dataareaid
from {{ source('fno', 'whsinventstatus') }} as whs
{%- if is_incremental() %}
    where whs.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where  whs.[IsDelete] is null
{% endif %}
