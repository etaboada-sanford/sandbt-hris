{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_warehouse_sk']
) }}

select
    il.[Id] as dim_d365_warehouse_sk
    , il.recid as warehouse_recid
    , il.inventlocationid as warehouseid
    , il.name as warehouse_name
    , upper(il.dataareaid) as warehouse_dataareaid
    , il.inventlocationidtransit
    , il.inventlocationlevel
    , il.inventlocationtype

    , st.recid as inventsite_recid
    , st.siteid
    , st.name as site_name
    , upper(st.dataareaid) as site_dataareaid
    , st.timezone
    , st.defaultdimension
    , st.defaultinventstatusid
    , st.dxc_defaultdimension
    , il.dxc_manufacturerid as manufacturerid
    , {{ translate_enum('ewt', 'il.dxc_edecwarehousetype' ) }} as warehousetype
    , {{ translate_enum('eot', 'il.dxc_operationstype' ) }} as operationstype

    , case when il.dxc_edecwarehousetype in (2, 3) then 1 else 0 end as is_vessel /* ('EDECWHType_DW_VESSEL' ,'EDECWHType_LP_VESSEL')*/

    , case when (upper(il.name) like '%CREDIT%' or upper(il.name) like '%MANAGE%') then 1 else 0 end as is_credit /* ticket #58648 */

            /* Operations type is not N/A then Production warehouse
            then if 3PL/STORAGE or COOL warehouse is not Production
            Else is a Production warehouse
            */
             /* 0 = 3PL/STORAGE, 1 = COOL */
    , case when il.dxc_operationstype != 0 then 1 when il.dxc_edecwarehousetype in (0, 1) then 0 else 1 end as is_production

    , il.partition
    , il.[IsDelete]
    , il.versionnumber
    , il.sysrowversion
from {{ source('fno', 'inventlocation') }} as il
left join {{ source('fno', 'inventsite') }} as st on il.inventsiteid = st.siteid and upper(il.dataareaid) = upper(st.dataareaid) and st.[IsDelete] is null
cross apply dbo.f_get_enum_translation('inventlocation', '1033') as eot
cross apply dbo.f_get_enum_translation('inventlocation', '1033') as ewt

{%- if is_incremental() %}
    where il.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where  il.[IsDelete] is null
{% endif %}

union all
/* Null warehouse for un-mapped from Navision */
select
    {{ dbt_utils.generate_surrogate_key([999999, "\'NAV999\'"]) }} as dim_d365_warehouse_sk
    , 999999 as warehouse_recid
    , 'NAV999' as warehouseid
    , 'Navision not migrated' as warehouse_name
    , 'SANF' as warehouse_dataareaid
    , null as inventlocationidtransit
    , null as inventlocationlevel
    , null as inventlocationtype
    , 999999 as inventsite_recid
    , 'NAV999' as siteid
    , 'Navision not migrated' as site_name
    , 'SANF' as site_dataareaid
    , null as timezone
    , null as defaultdimension
    , null as defaultinventstatusid
    , null as dxc_defaultdimension
    , null as manufacturerid
    , null as warehousetype
    , null as operationstype
    , 0 as is_vessel /* ('EDECWHType_DW_VESSEL' ,'EDECWHType_LP_VESSEL')*/
    , 0 as is_credit /* ticket #58648 */
    , 0 as is_production
    , null as partition -- noqa: RF04
    , null as [IsDelete]
    , 0 as versionnumber
    , 0 as sysrowversion
