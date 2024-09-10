{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_productionorder_sk']
) }}

select
    pt.[Id] as dim_d365_productionorder_sk
    , pt.recid as productionorder_recid
    , pt.prodid
    , pt.dxc_parentprodid as parentprodid
    , pt.prodpoolid
    , {{ translate_enum('eppt', 'pt.prodpostingtype') }} as prodpostingtype
    , {{ translate_enum('eps', 'pt.prodstatus') }} as prodstatus
    , {{ translate_enum('epbs', 'pt.backorderstatus') }} as backorderstatus
    , pt.dxc_salesid as salesid
    , pt.dxc_salesinventtransid as salesinventtransid
    , sl.qtyordered as salesline_qtyordered
    , pt.bomid
    , pt.collectreflevel
    , pt.collectrefprodid
    , pt.latestscheddirection
    , pt.schedstatus
    , pt.inventdimid
    , pt.inventrefid
    , pt.inventreftransid
    , pt.inventtransid
    , pt.itemid
    , pt.qtycalc
    , pt.qtysched
    , pt.qtystup
    , pt.remaininventphysical
    , pt.dxc_salescarriercode as salescarriercode
    , pt.dxc_salespoolid as salespoolid
    , pt.dxc_packagingunit as packagingunit
    , pt.dxc_packagingqty as packagingqty
    , case when pt.dxc_saleslinenumber = 0 then null else pt.dxc_saleslinenumber end as saleslinenumber
    , upper(sl.salesunit) as salesline_unit
    , round(case when upper(sl.salesunit) = 'LB' then sl.qtyordered * 0.45359237 else sl.qtyordered end, 3) as salesline_qtyordered_kg
    , cast(pt.bomdate as date) as bomdate
    , cast(pt.calcdate as date) as calcdate
    , case when cast(pt.dlvdate as date) = '1900-01-01' then null
        else cast(concat(convert(varchar(10), cast(pt.dlvdate as date), 120), ' ', convert(varchar(8), dateadd(second, pt.dlvtime, 0), 108)) as datetime2(0))
    end as deliverydatetime
    , case when cast(pt.finisheddate as date) = '1900-01-01' then null else cast(pt.finisheddate as date) end as finisheddate
    , cast(concat(convert(varchar(10), cast(pt.latestscheddate as date), 120), ' ', convert(varchar(8), dateadd(second, pt.latestschedtime, 0), 108)) as datetime2(0)) as latestscheduled_datetime
    , case when cast(pt.scheddate as date) = '1900-01-01' then null else cast(pt.scheddate as date) end as scheddate
    , case when cast(pt.schedend as date) = '1900-01-01' then null else cast(pt.schedend as date) end as schedenddate
    , case when cast(pt.schedstart as date) = '1900-01-01' then null else cast(pt.schedstart as date) end as schedstartdate
    , cast(dateadd(second, pt.schedfromtime, 0) as time(0)) as schedfromtime
    , cast(dateadd(second, pt.schedtotime, 0) as time(0)) as schedtotime
    , case when cast(pt.realdate as date) = '1900-01-01' then null else cast(pt.realdate as date) end as realdate
    , case when cast(pt.releaseddate as date) = '1900-01-01' then null else cast(pt.releaseddate as date) end as releaseddate
    , case when cast(pt.dlvdate as date) > '1900-01-01' then cast(pt.dlvdate as date)
        else cast(pt.releaseddate as date)
    end as reportdate
    , coalesce(eirt.[LocalizedLabel], 'None') as inventreftype
    , upper(pt.dataareaid) as prod_dataareaid
    , pt.versionnumber
    , pt.sysrowversion
from {{ source('fno', 'prodtable') }} as pt

left join
    {{ source('fno', 'salesline') }}
        as sl
    on pt.dxc_salesid = sl.salesid
        and pt.dxc_saleslinenumber = sl.linenum
        and pt.dataareaid = sl.dataareaid
        and sl.[IsDelete] is null

/* ProdStatus -> ProdStatus */
cross apply dbo.f_get_enum_translation('prodtable', '1033') as eps
    on eps.[OptionSetName] = 'prodstatus'
        and pt.prodstatus = eps.[Option]
        and eps.[EntityName] = 'prodtable'
/* backorderstatus -> ProdBackStatus */
cross apply dbo.f_get_enum_translation('prodtable', '1033') as epbs
    on epbs.[OptionSetName] = 'backorderstatus'
        and pt.backorderstatus = epbs.[Option]
        and epbs.[EntityName] = 'prodtable'
/* ProdPostingType -> ProdPostingType */
cross apply dbo.f_get_enum_translation('prodtable', '1033') as eppt
    on eppt.[OptionSetName] = 'prodpostingtype'
        and pt.prodpostingtype = eppt.[Option]
        and eppt.[EntityName] = 'prodtable'
/* InventRefType -> InventRefType */
cross apply dbo.f_get_enum_translation('prodtable', '1033') as eirt
    on eirt.[OptionSetName] = 'inventreftype'
        and pt.inventreftype = eirt.[Option]
        and eirt.[EntityName] = 'prodtable'
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
