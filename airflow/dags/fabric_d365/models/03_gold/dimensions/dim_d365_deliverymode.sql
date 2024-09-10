{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_deliverymode_sk']
) }}

select
    dlv.[Id] as dim_d365_deliverymode_sk
    , dlv.recid as deliverymode_recid
    , dlv.code as deliverymode
    , dlv.txt as deliverymode_desc
    , dlv.partition
    , dlv.[IsDelete]
    , dlv.versionnumber
    , dlv.sysrowversion
    , upper(dlv.dataareaid) as deliverymode_dataareaid
from {{ source('fno', 'dlvmode') }} as dlv
{%- if is_incremental() %}
    where dlv.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where dlv.[IsDelete] is null
{% endif %}
