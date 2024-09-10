with cte as (
        select 
            ip.recid posting_recid,
            f.dim_d365_financialdimensionaccount_sk,
            m.dim_d365_mainaccount_sk,
            en.dim_d365_enum_sk,
            cat.dim_d365_category_sk,
            ig.dim_d365_itemgroup_sk,
            itm.dim_d365_item_sk,
            upper(ip.dataareaid) dataareaid,

            en.enum_item_name,
            en.enumitemlabel,
            ip.categoryrelation,
            cat.category_name,
            ig.itemgroupid,
            itm.itemid
        from {{source('fno', 'inventposting')}} ip
        join {{ref('dim_d365_enum')}} en on ip.inventaccounttype = en.enum_item_value
            and upper(en.[OptionSetName]) = 'inventaccounttype'
            and upper(en.[EntityName]) = 'inventposting'
            and enum_item_value in (21)  /* 21 goes against the sales issue , thats the only one reqd for sales lines - others are not relevent and returns many to many records */

        left join {{ref('dim_d365_itemgroup')}} ig on ip.itemrelation = ig.itemgroupid and upper(ip.dataareaid) = upper(ig.itemgroup_dataareaid)
        left join {{ref('dim_d365_item')}} itm on ip.itemrelation  = itm.itemgroupid  and upper(ip.dataareaid) = upper(itm.item_dataareaid)
        left join {{ref('dim_d365_financialdimensionaccount')}} f on ip.ledgerdimension = f.financialdimensionaccount_recid
        left join {{ref('dim_d365_mainaccount')}} m on f.mainaccount = m.mainaccount_recid
        left join {{ref('dim_d365_category')}} cat on ip.categoryrelation = cat.category_recid  
)
select distinct 
        dim_d365_item_sk,
        dim_d365_mainaccount_sk,
        dataareaid 
from cte