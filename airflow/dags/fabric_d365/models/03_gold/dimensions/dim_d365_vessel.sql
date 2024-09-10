{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_vessel_sk']
) }}

select
    dv.mserp_dxc_vesselentityid as dim_d365_vessel_sk
    , dv.mserp_dxc_vesselentityid as vessel_recid -- todo
    , dv.mserp_vesselid as vesselid
    , dv.mserp_vesselname as vesselname
    , dv.mserp_triptypeid as triptypeid
    , dv.mserp_tripidnumbersequencecode as tripidnumbersequencecode
    , dv.mserp_inventlocationid as inventlocationid
    , dv.mserp_vendaccount as vendaccount
    , dv.mserp_equipment as equipment
    , dv.mserp_vesselcallsign as vesselcallsign
    , dv.mserp_createpurchaseorder as createpurchaseorder
    , dv.mserp_createtransferorder as createtransferorder
    , dv.mserp_sanfordvessel as sanfordvessel
    , null as createddatetime -- todo
    , null as createdby -- todo
    , dv.mserp_faoareaid as faoareaid
    , dv.mserp_createproductionorders as createproductionorders
    , null as [partition] -- todo
    , dv.[IsDelete]
    /* case when replace(tripidnumbersequencecode,'TRP_','')=inventlocationid then 1 else 0 end is_warehouse, */
    , 0 as versionnumber
    , 0 as sysrowversion
    , case when dv.mserp_triptypeid = 'DEEP WATER SCAMPI' then 'SCAMPI'
        when dv.mserp_triptypeid = 'DEEP WATER' and dv.mserp_equipment = 'BLL' then 'LONGLINE'
        when dv.mserp_triptypeid = 'DEEP WATER' and dv.mserp_equipment = 'BT' then 'FACTORY'
        else ''
    end as vessel_type
    , upper(dv.mserp_dataareaid) as vessel_dataareaid
    , case when w.is_vessel = 1 then 1 else 0 end as is_warehouse
from {{ source('mserp', 'dxc_vessel') }} as dv
left join {{ ref('dim_d365_warehouse') }} as w
    on dv.mserp_inventlocationid = w.warehouseid
        and w.warehouse_dataareaid = upper(dv.mserp_dataareaid)
        and w.[IsDelete] is null

