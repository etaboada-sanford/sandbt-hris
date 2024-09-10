{{ config(
    materialized = 'incremental', 
    unique_key = ['exchangerate_recid']
) }}

/*
ItemCode:
0   Table - ItemId
1   Group - ItemGroupId
2   All
3   Category - Use EcoResCategory link

selection order 0, 1, 3, 2

CustVendCode:
0   Table - Customer / Vendor AccountNum
1   Group - CustGroupID / VendGroupId
2   All

selection order 0, 1, 2
*/

with inventpostingmap as (
    select distinct

        ip.inventaccounttype
        , en.enum_item_name as inventpostingtype
        , en.enumitemlabel
            as inventpostingname

        , case
            when ip.inventaccounttype in (
                    19      /* Cost of units, delivered | SalesPackingSlip */
                    , 20    /* Cost of goods sold, delivered | SalesPackingSlipOffsetAccount */
                    , 21    /* Cost of units, invoiced | SalesIssue */
                    , 22    /* Cost of goods sold, invoiced | SalesConsump */
                    , 23    /* Sales order, revenue SalesRevenue */
                ) then 'Sales order'

            when ip.inventaccounttype in (
                    0      /* Cost of purchased materials received | PurchPackingSlip */
                    , 1    /* Purchase expenditure, un-invoiced | PurchPackingSlipOffsetAccount */
                    , 2    /* Cost of purchased materials invoiced	| PurchReceipt */
                    , 3    /* Purchase expenditure for product | PurchConsump */
                    , 33   /* Stock variation | PurchStockVariation */
                    , 34   /* Purchase, accrual | PurchPackingSlipPurchaseOffset */
                    , 62   /* Prepayment | PurchAdvance */
                    , 63   /* Purchase expenditure for expense | PurchExpense */
                    , 110   /* Landed cost goods in transit | PurchITMGIT */
                ) then 'Purchase order'

            when ip.inventaccounttype in (
                    7      /* Inventory issue | InventIssue */
                    , 8    /* Inventory expenditure, loss | InventLoss */
                    , 9    /* Inventory receipt | InventReceipt */
                    , 10   /* Inventory expenditure, profit	| InventProfit */
                    , 44   /* Inventory inter-unit payable | InventInterUnitPayable */
                    , 45   /* Inventory inter-unit receivable | InventInterUnitReceivable */
                ) then 'Inventory'

            when ip.inventaccounttype in (
                    11      /* Estimated cost of materials consumed	| ProdPicklist */
                    , 12    /* Estimated cost of materials consumed, WIP | ProdPicklistOffsetAccount */
                    , 13    /* Estimated manufactured cost | ProdReportFinished */
                    , 14    /* Estimated manufactured cost, WIP | ProdReportFinishedOffsetAccoun */
                    , 15    /* Cost of materials consumed | ProdIssue */
                    , 16    /* Cost of materials consumed, WIP | ProdIssueOffsetAccount */
                    , 17    /* Manufactured cost | ProdReceipt */
                    , 18    /* Manufactured cost, WIP | ProdReceiptOffsetAccount */
                ) then 'Production'


            when ip.inventaccounttype in (
                    39      /* Physical purchase price variance | PurchStdCostPurchasePriceVaria */
                    , 38    /* Inventory cost revaluation | InventStdCostRevaluation */
                    , 46    /* Cost change variance | InventStdCostChangeVariance */
                    , 47    /* Production lot size variance | ProdStdCostLotSizeVariance */
                    , 41    /* Production price variance | ProdStdCostProductionVariance */
                    , 48    /* Production quantity variance | ProdStdCostQuantityVariance */
                    , 49    /* Production substitution variance | ProdStdCostSubstitutionVarianc */
                    , 50    /* Rounding variance | InventStdCostRoundingVariance */
                ) then 'Standard cost variance'

        end as postinggroup
        , upper(ip.dataareaid) as dataareaid
        , ip.versionnumber
        , ip.sysrowversion
    from {{ source('fno', 'inventposting') }} as ip
    inner join {{ ref('dim_d365_enum') }} as en on ip.inventaccounttype = en.enum_item_value and upper(en.enumname) = 'inventpostinginventaccounttype'
    where upper(ip.dataareaid) in ('SANF', 'SANA')
)

, itemposting_item as (
    /* ItemCode = 0 - direct link to Item */
    select
        ip.recid as posting_recid
        , itm_itemcode.item_recid
        , ip.inventaccounttype

        , map.postinggroup
        , map.inventpostingtype
        , map.inventpostingname

        , ip.categoryrelation
        , ip.custvendcode
        , ip.custvendrelation

        , ip.itemcode
        , ip.itemrelation
        , itm_itemcode.itemid
        , itm_itemcode.dim_d365_item_sk
        , m.mainaccountid
        , m.dim_d365_mainaccount_sk
        , upper(ip.dataareaid) as dataareaid

    from {{ source('fno', 'inventposting') }} as ip

    inner join {{ ref('dim_d365_financialdimensionaccount') }} as f on ip.ledgerdimension = f.financialdimensionaccount_recid
    inner join {{ ref('dim_d365_mainaccount') }} as m on f.mainaccount = m.mainaccount_recid

    /* ItemCode = 0 - direct link to Item */
    inner join
        {{ ref('dim_d365_item') }}
            as itm_itemcode
        on ip.itemrelation = itm_itemcode.itemid
            and upper(ip.dataareaid) = upper(itm_itemcode.item_dataareaid)

    inner join
        inventpostingmap
            as map
        on ip.inventaccounttype = map.inventaccounttype
            and upper(ip.dataareaid) = map.dataareaid

    where ip.itemcode = 0
        and ip.costrelation is null /* do not include cost groups */
        and upper(ip.dataareaid) in ('SANF', 'SANA')
)

, itemposting_group as (
    /* ItemCode = 1 - Itemgroup link to Item */
    select
        ip.recid as posting_recid
        , itm_itemgroup.item_recid
        , ip.inventaccounttype

        , map.postinggroup
        , map.inventpostingtype
        , map.inventpostingname

        , ip.categoryrelation
        , ip.custvendcode
        , ip.custvendrelation

        , ip.itemcode
        , ip.itemrelation
        , itm_itemgroup.itemid
        , itm_itemgroup.dim_d365_item_sk
        , m.mainaccountid
        , m.dim_d365_mainaccount_sk
        , upper(ip.dataareaid) as dataareaid

    from {{ source('fno', 'inventposting') }} as ip

    inner join {{ ref('dim_d365_financialdimensionaccount') }} as f on ip.ledgerdimension = f.financialdimensionaccount_recid
    inner join {{ ref('dim_d365_mainaccount') }} as m on f.mainaccount = m.mainaccount_recid

    /* ItemCode = 1 - Itemgroup link to Item */
    inner join
        {{ ref('dim_d365_item') }}
            as itm_itemgroup
        on ip.itemrelation = itm_itemgroup.itemgroupid
            and upper(ip.dataareaid) = upper(itm_itemgroup.item_dataareaid)

    inner join
        inventpostingmap
            as map
        on ip.inventaccounttype = map.inventaccounttype
            and upper(ip.dataareaid) = map.dataareaid

    where ip.itemcode = 1
        and ip.costrelation is null /* do not include cost groups */
        and upper(ip.dataareaid) in ('SANF', 'SANA')
)

-- select * from itemposting_group;

, itemposting_all as (
    /* ItemCode = 2 - ALL Items  - this is the last level 'catch-all' */
    select
        ip.recid as posting_recid
        , itm_all.item_recid
        , ip.inventaccounttype

        , map.postinggroup
        , map.inventpostingtype
        , map.inventpostingname

        , ip.categoryrelation
        , ip.custvendcode
        , ip.custvendrelation

        , ip.itemcode
        , ip.itemrelation
        , itm_all.itemid
        , itm_all.dim_d365_item_sk
        , m.mainaccountid
        , m.dim_d365_mainaccount_sk
        , upper(ip.dataareaid) as dataareaid

    from {{ source('fno', 'inventposting') }} as ip

    inner join {{ ref('dim_d365_financialdimensionaccount') }} as f on ip.ledgerdimension = f.financialdimensionaccount_recid
    inner join {{ ref('dim_d365_mainaccount') }} as m on f.mainaccount = m.mainaccount_recid

    /* ItemCode = 2 - ALL Items */
    inner join
        {{ ref('dim_d365_item') }}
            as itm_all
        on ip.itemrelation is null
            and upper(ip.dataareaid) = upper(itm_all.item_dataareaid)

    inner join
        inventpostingmap
            as map
        on ip.inventaccounttype = map.inventaccounttype
            and upper(ip.dataareaid) = map.dataareaid

    where ip.itemcode = 2
        and ip.costrelation is null /* do not include cost groups */
        and upper(ip.dataareaid) in ('SANF', 'SANA')
        and ip.[IsDelete] is null
)

, itemposting_cat as (
    /* ItemCode = 3 - ItemCategory - PurchaseOrder only */
    select
        ip.recid as posting_recid
        , itm_cat.item_recid
        , ip.inventaccounttype

        , map.postinggroup
        , map.inventpostingtype
        , map.inventpostingname

        , ip.categoryrelation

        , ip.custvendcode
        , ip.custvendrelation

        , ip.itemcode
        , ip.itemrelation
        , itm_cat.itemid
        , itm_cat.dim_d365_item_sk
        , m.mainaccountid
        , m.dim_d365_mainaccount_sk
        , upper(ip.dataareaid) as dataareaid

    from {{ source('fno', 'inventposting') }} as ip

    inner join {{ ref('dim_d365_financialdimensionaccount') }} as f on ip.ledgerdimension = f.financialdimensionaccount_recid
    inner join {{ ref('dim_d365_mainaccount') }} as m on f.mainaccount = m.mainaccount_recid

    /* ItemCode = 3 - Category */

    inner join {{ ref('dim_d365_itemcategory') }} as cat on ip.categoryrelation = cat.category_recid

    inner join
        {{ ref('dim_d365_item') }}
            as itm_cat
        on cat.item_recid = itm_cat.item_recid
            and upper(ip.dataareaid) = upper(itm_cat.item_dataareaid)

    inner join
        inventpostingmap
            as map
        on ip.inventaccounttype = map.inventaccounttype
            and upper(ip.dataareaid) = map.dataareaid

    where ip.itemcode = 3
        and ip.costrelation is null /* do not include cost groups */
        and upper(ip.dataareaid) in ('SANF', 'SANA')
        and ip.[IsDelete] is null
)

/* categories for Non-items used in Purchase orders */
, itemposting_cat_nonitem as (
    /* ItemCode = 3 - ItemCategory - PurchaseOrder only */
    select
        ip.recid as posting_recid
        , cat.category_recid as item_recid
        , ip.inventaccounttype

        , map.postinggroup
        , map.inventpostingtype
        , map.inventpostingname

        , ip.categoryrelation

        , ip.custvendcode
        , ip.custvendrelation

        , ip.itemcode
        , ip.itemrelation
        , null as itemid
        , null as dim_d365_item_sk
        , m.mainaccountid
        , m.dim_d365_mainaccount_sk
        , upper(ip.dataareaid) as dataareaid

    from {{ source('fno', 'inventposting') }} as ip

    inner join {{ ref('dim_d365_financialdimensionaccount') }} as f on ip.ledgerdimension = f.financialdimensionaccount_recid
    inner join {{ ref('dim_d365_mainaccount') }} as m on f.mainaccount = m.mainaccount_recid

    /* ItemCode = 3 - Category */

    inner join {{ ref('dim_d365_category') }} as cat on ip.categoryrelation = cat.category_recid

    inner join
        inventpostingmap
            as map
        on ip.inventaccounttype = map.inventaccounttype
            and upper(ip.dataareaid) = map.dataareaid

    where ip.itemcode = 3
        and ip.costrelation is null /* do not include cost groups */
        and upper(ip.dataareaid) in ('SANF', 'SANA')
        and ip.[IsDelete] is null
)

/* unique records only */
, itemposting as (
    select *
    from itemposting_item

    union all

    select g.*
    from itemposting_group as g
    left join
        itemposting_item
            as i
        on g.inventaccounttype = i.inventaccounttype
            and g.item_recid = i.item_recid
            and i.custvendrelation is null
    where i.item_recid is null

    union all

    select c.*
    from itemposting_cat as c
    left join
        itemposting_item
            as i
        on c.inventaccounttype = i.inventaccounttype
            and c.item_recid = i.item_recid
            and i.custvendrelation is null
    left join
        itemposting_group
            as g
        on c.inventaccounttype = g.inventaccounttype
            and c.item_recid = g.item_recid
            and g.custvendrelation is null
    where i.item_recid is null
        and g.item_recid is null

    union all

    select a.*
    from itemposting_all as a
    left join
        itemposting_item
            as i
        on a.inventaccounttype = i.inventaccounttype
            and a.item_recid = i.item_recid
            and i.custvendrelation is null
    left join
        itemposting_group
            as g
        on a.inventaccounttype = g.inventaccounttype
            and a.item_recid = g.item_recid
            and g.custvendrelation is null
    left join
        itemposting_cat
            as c
        on a.inventaccounttype = c.inventaccounttype
            and a.item_recid = c.item_recid
            and c.custvendrelation is null
    where i.item_recid is null
        and g.item_recid is null
        and c.item_recid is null

    union all

    select *
    from itemposting_cat_nonitem
)

select
    {{ dbt_utils.generate_surrogate_key(['posting_recid', 'item_recid']) }} as fact_itemposting_sk
    , inventaccounttype
    , postinggroup
    , inventpostingtype
    , inventpostingname
    , categoryrelation
    , custvendcode
    , custvendrelation
    , itemcode
    , itemrelation
    , itemid
    , dim_d365_item_sk
    , mainaccountid
    , dim_d365_mainaccount_sk
    , dataareaid as itemposting_dataareaid
from itemposting
