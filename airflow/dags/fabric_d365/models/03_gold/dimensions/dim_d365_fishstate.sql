{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_fishstate_sk']
) }}

with uniquevalues as (
    select distinct mserp_dxc_stateentityid
    from {{ source('mserp', 'dxc_statetable') }}
)

, convertedvalues as (
    select
        mserp_dxc_stateentityid
        , row_number() over (order by mserp_dxc_stateentityid) as rownum
    from uniquevalues
)

, recids as (
    select
        mserp_dxc_stateentityid
        , cast('5637146076' + cast(rownum as varchar) as bigint) as recid
    from convertedvalues
)

select
    a.mserp_dxc_stateentityid as dim_d365_fishstate_sk
    , b.recid as fishstate_recid
    , a.mserp_dxc_statename as fishstatecode
    , a.mserp_dxc_mpiitemdesc as fishstate
    , a.mserp_dxc_mpistate as mpistate
    , null as [partition]
    , lower(a.mserp_dataareaid) as fishstate_dataareaid
    , null as [IsDelete]
    , 0 as versionnumber
    , 0 as sysrowversion
from {{ source('mserp', 'dxc_statetable') }} as a
inner join recids as b on a.mserp_dxc_stateentityid = b.mserp_dxc_stateentityid
where a.[IsDelete] is null
