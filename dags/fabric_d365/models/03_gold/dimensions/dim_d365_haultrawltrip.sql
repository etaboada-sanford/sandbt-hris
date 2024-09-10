{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_haultrawltrip_sk']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['mserp_primaryfield']) }} as dim_d365_haultrawltrip_sk

    , mserp_primaryfield as recid

    , mserp_tripid as tripid
    , mserp_haultrawlid as haultrawlid
    , null as recversion
    , null as modifieddatetime
    , null as modifiedby
    , null as createddatetime
    , null as createdby
    , mserp_catchharvestareaid as catchharvestareaid
    , mserp_faoareaid as faoareaid
    , mserp_fishinggearid as fishinggearid
    , mserp_landedportid as landedportid
    , mserp_mscid as mscid
    , upper(mserp_dataareaid) as haultrawltrip_dataareaid
    , null as [partition]
    , null as [IsDelete]
    , 0 as versionnumber
    , 0 as sysrowversion
from {{ source('mserp', 'dxc_haultrawl') }}
-- where [IsDelete] is null
