{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_item_sk']
) }}

with seafood_item_excl as (
    select distinct
        itemid
        , dataareaid
        , versionnumber
        , sysrowversion
    from {{ source('fno', 'inventtable') }}
    where
        (
            len(itemid) < 4 or itemid like '7%' or (itemid like '5%' and itemid not in ('50000835', '50000836', '50000837', '50000838', '50000839', '50000840', '50000841', '50000842', '50000843'))
        )
)

, ctitem1 as (
    select
        i.[Id] as dim_d365_item_sk
        , i.recid as item_recid
        , i.itemid
        , i.namealias
        , pt.name as item_name
        , i.itemtype
        , {{ translate_enum('enumit', 'i.itemtype' ) }} as itemtype_desc
        , igi.itemgroupid
        , ig.name as itemgroup_name
        , sdg.name as itemstoragedimensiongroupid
        , sdg.description as itemstoragedimensiongroup_name
        , tdg.name as itemtrackingdimensiongroupid
        , tdg.description as itemtrackingdimensiongroup_name
        /*
            BATCH =         InventBatchId,  not InventSerialID      isPhysicalInventory
            BATCHFI =       InventBatchId,  not InventSerialID      isFinancialInventory, isPhysicalInventory
            SERIAL =        InventBatchId,  InventSerialID          isPhysicalInventory
            SERIALFI =      InventBatchId,  InventSerialID          isFinancialInventory, isPhysicalInventory
            SERIALBLANK =   InventBatchId,  InventSerialID
            NONE =          not InventBatchId, not InventSerialID
        */
        , i.batchnumgroupid
        , i.bomcalcgroupid
        , i.commissiongroupid
        , i.costgroupid
        , i.itembuyergroupid
        , i.itempricetolerancegroupid
        , i.packaginggroupid
        , i.pdsfreightallocationgroupid
        , i.pdsitemrebategroupid
        , i.prodgroupid
        , i.reqgroupid as itemcoveragegroupid
        , i.serialnumgroupid
        , i.itmarrivalgroupid
        , i.itmoverundertolerancegroupid
        , i.itmcosttypegroupid
        , i.itmcosttransfergroupid
        , i.bomlevel
        , i.bomunitid
        , i.defaultdimension
        , i.netweight
        , i.productlifecyclestateid
        , i.salesmodel
        , i.dxc_speciescode as speciescode
        , i.dxc_mpistate as mpistate
        , i.dxc_mpistatecode as mpistatecode
        , mpist.mserp_dxc_mpiitemdesc as mpistate_desc
        , i.dxc_mpiunit as mpiunit
        , i.dxc_appliedweight as appliedweight
        , {{ translate_enum('aw', 'i.dxc_appliedweight' ) }} as appliedweight_name
        , {{ translate_enum('aw', 'i.dxc_appliedweight' ) }} as appliedweight_label
        , i.dxc_maximumweight as maximumweight
        , i.dxc_storagecondition as storagecondition -- [appliedweight_name]
        , {{ translate_enum('isc', 'i.dxc_storagecondition' ) }} as storagecondition_name
        , i.dxc_legacyitemcode as legacyitemcode
        , i.dxc_labelformat as labelformat
        , i.dxc_sizename as sizename
        , i.dxc_licence as licence
        , i.dxc_packagingcode as packagingcode
        , i.dxc_packagingcode as packagingshortname
        , i.dxc_primarypackagingtype as primarypackagingtype
        , i.dxc_masterpackagingtype as masterpackagingtype
        , i.dxc_processstatecode as processstatecode
        , i.dxc_qualityname as qualityname
        , i.dxc_batchattributegroupid as batchattributegroupid
        , i.dxc_autopurchinvoiceconsitems as is_consignmentitemautopurchinvoice
        , i.product
        , {{ translate_enum('enum', 'p.producttype' ) }} as producttype
        , i.partition
        , i.recversion
        , i.modifiedon as lastprocessedchange_datetime
        , ic.category_l_1 as planning_category_l_1
        , ic.category_l_2 as planning_category_l_2
        , ic.category_l_3 as planning_category_l_3
        , ic.category_l_4 as planning_category_l_4
        , ic.category_l_5 as planning_category_l_5
        , ic.category_l_6 as planning_category_l_6
        , ic.category_l_7 as planning_category_l_7
        , ic.category_path as planning_category_path
        , lcst.unitcost as latest_transit_unitcost
        , ia.allocation
        , ia.sales_allocation
        , i.[IsDelete]
        , upper(i.dataareaid) as item_dataareaid
        , concat(i.itemid, ' - ', pt.name) as item_desc
        , concat(fdv.d1_businessunitvalue, ' - ', fdv.d1_businessunit_name) as default_businessunit
        /* dxc_qualityname is not a mandatory field, batchattributegroupid is a mandatory field as per process */
        , case when invenbl.itemid is not null then 1 end as whsinventenabled
        , case when lower(i.namealias) like 'fish bin%' and upper(igi.itemgroupid) = 'SUNDRY' then 1 else 0 end as is_bin
        , concat(i.dxc_speciescode, i.dxc_processstatecode) as item_group_code
        , case when seafood_item_excl.itemid is null then 'Yes' else 'No' end as seafooditem
        , case
            when i.dxc_qualityname in ('BAIT') and i.dxc_batchattributegroupid like '%CP%' then 'Bait - CP'
            when i.dxc_qualityname in ('BAIT') and i.dxc_batchattributegroupid not like '%CP%' then 'Bait'
            when i.dxc_qualityname in ('FISHMEAL', 'FFM') and i.dxc_batchattributegroupid like '%CP%' then 'Fish Meal - CP'
            when i.dxc_qualityname in ('FISHMEAL', 'FFM') and i.dxc_batchattributegroupid not like '%CP%' then 'Fish Meal'
            when (i.dxc_qualityname not in ('BAIT', 'FISHMEAL', 'FFM') or i.dxc_qualityname is null) and i.dxc_batchattributegroupid not like '%NHC' and i.dxc_batchattributegroupid like '%CP%' then 'Edible - CP'
            when (i.dxc_qualityname not in ('BAIT', 'FISHMEAL', 'FFM') or i.dxc_qualityname is null) and i.dxc_batchattributegroupid not like '%NHC' and i.dxc_batchattributegroupid not like '%CP%' then 'Edible'
            when (i.dxc_qualityname not in ('BAIT', 'FISHMEAL', 'FFM') or i.dxc_qualityname is null) and i.dxc_batchattributegroupid like '%NHC' and i.dxc_batchattributegroupid like '%CP%' then 'Indible - CP'
            when (i.dxc_qualityname not in ('BAIT', 'FISHMEAL', 'FFM') or i.dxc_qualityname is null) and i.dxc_batchattributegroupid like '%NHC' and i.dxc_batchattributegroupid not like '%CP%' then 'Inedible'
            else 'Others'
        end as sales_type
        , case when i.itemid like '10%' then '10s'
            when i.itemid like '20%' then '20s'
            when i.itemid like '30%' then '30s'
            else
                'Misc'
        end as items_10s20s30s

    from {{ source('fno', 'inventtable') }} as i
    left join
        {{ source('fno', 'ecoresproduct') }}
            as p
        on i.product = p.recid
            and p.[IsDelete] is null
    left join
        {{ source('fno', 'ecoresproducttranslation') }}
            as pt
        on i.product = pt.recid
            and pt.languageid = 'en-NZ'
            and pt.[IsDelete] is null
    left join
        {{ source('fno', 'inventitemgroupitem') }}
            as igi
        on i.itemid = igi.itemid
            and upper(i.dataareaid) = upper(igi.itemdataareaid)
            and igi.[IsDelete] is null
    left join
        {{ source('fno', 'inventitemgroup') }}
            as ig
        on igi.itemgroupid = ig.itemgroupid
            and upper(i.dataareaid) = upper(ig.dataareaid)
            and ig.[IsDelete] is null
    left join
        {{ source('fno', 'ecoresstoragedimensiongroupitem') }}
            as sdig
        on i.itemid = sdig.itemid
            and upper(i.dataareaid) = upper(sdig.itemdataareaid)
            and sdig.[IsDelete] is null
    left join
        {{ source('fno', 'ecoresstoragedimensiongroup') }}
            as sdg
        on sdig.storagedimensiongroup = sdg.recid
            and sdg.[IsDelete] is null

    left join
        {{ source('fno', 'ecorestrackingdimensiongroupitem') }}
            as tdig
        on i.itemid = tdig.itemid
            and upper(i.dataareaid) = upper(tdig.itemdataareaid)
            and tdig.[IsDelete] is null
    left join
        {{ source('fno', 'ecorestrackingdimensiongroup') }}
            as tdg
        on tdig.trackingdimensiongroup = tdg.recid
            and tdg.[IsDelete] is null

    left join
        {{ source('fno', 'whsinventenabled') }}
            as invenbl
        on i.itemid = invenbl.itemid
            and upper(invenbl.dataareaid) = upper(i.dataareaid)
            and invenbl.[IsDelete] is null
    cross apply dbo.f_get_enum_translation('inventtable', '1033') as enumit
    cross apply dbo.f_get_enum_translation('ecoresproduct', '1033') as enum

    /* adding planning product categories against the dim as per commercial finance requirements */
    left join
        {{ ref('dim_d365_itemcategory') }}
            as ic
        on i.recid = ic.item_recid
            and upper(i.dataareaid) = ic.itemcategory_dataareaid
            and ic.item_category = 'Planning'

    /* packaging code config disabling because multiple records for same code eg 'SAM FIL 10kg'
    --left join { { s o u r ce('fno', 'dxc_packagingconfigurationtable') }} pck on i.dxc_packagingcode = pck.packagingcode and upper(i.dataareaid) = upper(pck.dataareaid)
    --left join { { r e f ('stg_dim_d365_item_std_cost_1') }} icst on i.itemid = icst.itemid and upper(i.dataareaid)  = upper(icst.item_standardcost_dataareaid)
    */
    cross apply dbo.f_get_enum_translation('inventtable', '1033') as isc
    cross apply dbo.f_get_enum_translation('inventtable', '1033') as aw

    left join
        {{ ref('stg_dim_d365_item_allocation') }}
            as ia
        on i.itemid = ia.itemid and ia.item_allocation_dataarea = upper(i.dataareaid)
            and ia.allocation_rank_unique = 1 and ia.allocation_rank = 1

    left join {{ source('mserp', 'dxc_statetable') }} as mpist
        on i.dxc_mpistate = mpist.mserp_dxc_mpistate and mpist.[IsDelete] is null
            and upper(mpist.mserp_dataareaid) = upper(i.dataareaid)
            and upper(mpist.mserp_dxc_statename) = upper(i.dxc_mpistatecode)

    left join {{ ref('dim_d365_financialdimensionvalueset') }} as fdv on i.defaultdimension = fdv.financialdimensionvalueset_recid

    left join
        {{ ref('stg_fct_d365_inventitemprice_latest') }}
            as lcst
        on i.itemid = lcst.itemid and upper(i.dataareaid) = lcst.inventitemprice_dataareaid
            and lcst.inventsiteid = 'TRANSIT'

    left join seafood_item_excl on i.itemid = seafood_item_excl.itemid and upper(i.dataareaid) = upper(seafood_item_excl.dataareaid)

    where i.[IsDelete] is null

)

select
    dim_d365_item_sk
    , item_recid
    , item_dataareaid
    , itemid
    , namealias
    , item_name
    , item_desc
    , itemtype
    , itemtype_desc
    , itemgroupid
    , itemgroup_name
    , itemstoragedimensiongroupid
    , itemstoragedimensiongroup_name
    , itemtrackingdimensiongroupid
    , itemtrackingdimensiongroup_name
    , batchnumgroupid
    , bomcalcgroupid
    , commissiongroupid
    , costgroupid
    , itembuyergroupid
    , itempricetolerancegroupid
    , packaginggroupid
    , pdsfreightallocationgroupid
    , pdsitemrebategroupid
    , prodgroupid
    , itemcoveragegroupid
    , serialnumgroupid
    , itmarrivalgroupid
    , itmoverundertolerancegroupid
    , itmcosttypegroupid
    , itmcosttransfergroupid
    , bomlevel
    , bomunitid
    , defaultdimension
    , default_businessunit
    , netweight
    , productlifecyclestateid
    , salesmodel
    , speciescode
    , mpistate
    , mpistatecode
    , mpistate_desc
    , mpiunit
    , appliedweight
    , appliedweight_name
    , appliedweight_label
    , maximumweight
    , storagecondition
    , storagecondition_name
    , legacyitemcode
    , labelformat
    , sizename
    , licence
    , packagingcode
    , packagingshortname
    , primarypackagingtype
    , masterpackagingtype
    , processstatecode
    , qualityname
    , batchattributegroupid
    , is_consignmentitemautopurchinvoice
    , product
    , producttype
    , whsinventenabled
    , is_bin
    , partition
    , recversion
    , lastprocessedchange_datetime
    , planning_category_l_1
    , planning_category_l_2
    , planning_category_l_3
    , planning_category_l_4
    , planning_category_l_5
    , planning_category_l_6
    , planning_category_l_7
    , planning_category_path
    , item_group_code
    , seafooditem
    , items_10s20s30s
    , latest_transit_unitcost
    , allocation
    , sales_allocation
    , [IsDelete]
    , case when sales_type in ('Bait - CP', 'Bait', 'Fish Meal - CP', 'Fish Meal') then 'No' else 'Yes' end as sales_target_item_flag
from ctitem1
union all
select
    dim_d365_item_sk
    , item_recid
    , item_dataareaid
    , itemid
    , namealias
    , item_name
    , item_desc
    , itemtype
    , itemtype_desc
    , itemgroupid
    , itemgroup_name
    , itemstoragedimensiongroupid
    , itemstoragedimensiongroup_name
    , itemtrackingdimensiongroupid
    , itemtrackingdimensiongroup_name
    , batchnumgroupid
    , bomcalcgroupid
    , commissiongroupid
    , costgroupid
    , itembuyergroupid
    , itempricetolerancegroupid
    , packaginggroupid
    , pdsfreightallocationgroupid
    , pdsitemrebategroupid
    , prodgroupid
    , itemcoveragegroupid
    , serialnumgroupid
    , itmarrivalgroupid
    , itmoverundertolerancegroupid
    , itmcosttypegroupid
    , itmcosttransfergroupid
    , bomlevel
    , bomunitid
    , defaultdimension
    , default_businessunit
    , netweight
    , productlifecyclestateid
    , salesmodel
    , speciescode
    , mpistate
    , mpistatecode
    , mpistate_desc
    , mpiunit
    , appliedweight
    , appliedweight_name
    , appliedweight_label
    , maximumweight
    , storagecondition
    , storagecondition_name
    , legacyitemcode
    , labelformat
    , sizename
    , licence
    , packagingcode
    , packagingshortname
    , primarypackagingtype
    , masterpackagingtype
    , processstatecode
    , qualityname
    , batchattributegroupid
    , is_consignmentitemautopurchinvoice
    , product
    , producttype
    , whsinventenabled
    , is_bin
    , partition
    , recversion
    , lastprocessedchange_datetime
    , planning_category_l_1
    , planning_category_l_2
    , planning_category_l_3
    , planning_category_l_4
    , planning_category_l_5
    , planning_category_l_6
    , planning_category_l_7
    , planning_category_path
    , item_group_code
    , seafooditem
    , items_10s20s30s
    , latest_transit_unitcost
    , allocation
    , sales_allocation
    , [IsDelete]
    , sales_target_item_flag
from {{ ref('stg_nav_dummy_salesitem') }}
