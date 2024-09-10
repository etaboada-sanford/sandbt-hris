{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_inventitemprice_sk']
) }}

select
    ip.[Id] as dim_d365_inventitemprice_sk
    , ip.recid
    , ip.costingtype
    , ip.itemid
    , ip.markup
    , ip.priceallocatemarkup
    , ip.pricecalcid
    , ip.priceqty
    , ip.pricetype
    , ip.priceunit
    , ip.activationdate
    , ip.createddatetime
    , ip.unitid
    , ip.versionid
    , ip.dataareaid
    , ip.dxc_inventitempricesim
    , ip.[IsDelete]
    , ip.versionnumber
    , ip.sysrowversion
    , round(ip.price, 2) as price
from {{ source('fno', 'inventitemprice') }} as ip
{%- if is_incremental() %}
    where ip.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where ip.[IsDelete] is null
{% endif %}
