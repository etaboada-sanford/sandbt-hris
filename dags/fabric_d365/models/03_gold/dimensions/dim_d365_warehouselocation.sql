{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_warehouselocation_sk']
) }}

with ctw as (
    select
        whl.[Id] as dim_d365_warehouselocation_sk
        , whl.recid as warehouselocation_recid
        , whl.wmslocationid as warehouselocationid
        , whl.absoluteheight
        , whl.aisleid
        , whl.depth
        , whl.height
        , whl.volume
        , whl.width
        , whl.inputlocation
        , whl.inventlocationid as warehouseid
        , whl.locprofileid
        , whl.locationtype
        , whl.zoneid
        , whl.level
        , whl.partition
        , whl.[IsDelete]
        , whl.versionnumber
        , whl.sysrowversion
        , upper(whl.dataareaid) as warehouselocation_dataareaid
    from {{ source('fno', 'wmslocation') }} as whl
    {%- if is_incremental() %}
        where whl.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where  whl.[IsDelete] is null
    {% endif %}
)

select
    *
    , case
        when zoneid like 'CHILLER%' then 1
        when warehouseid = 'HAV01' and warehouselocationid = 'RECV' then 1
        when warehouseid = 'NIML' and warehouselocationid = 'PRDDEF' then 1
        when warehouseid = 'BNE02' and warehouselocationid = 'INWCHI' then 1
        when warehouseid = 'BNE02' and warehouselocationid = 'CHIWIP' then 1
        when warehouseid = 'M04' and warehouselocationid = 'DEF' then 1
        when warehouseid = 'M12' and warehouselocationid = 'DEF' then 1
        when warehouseid = 'TIM01' and warehouselocationid = 'INWCHI' then 1
        else 0
    end as ischiller
from ctw
