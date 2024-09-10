{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_inventdim_sk']
) }}

select
    id.[Id] as dim_d365_inventdim_sk
    , id.recid as inventdim_recid
    , id.inventdimid
    , id.inventbatchid
    , id.inventlocationid
    , id.wmslocationid
    , id.inventserialid
    , id.inventsiteid
    , id.inventstatusid
    , id.inventversionid
    , id.inventstyleid
    , id.licenseplateid
    , id.wmspalletid
    , id.createddatetime
    , '' as partitionid
    , id.[IsDelete]
    , id.versionnumber
    , id.sysrowversion
    , upper(id.dataareaid) as inventdim_dataareaid
from
    {{ source('fno', 'inventdim') }} as id
{%- if is_incremental() %}
    where id.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where id.[IsDelete] is null
{% endif %}
