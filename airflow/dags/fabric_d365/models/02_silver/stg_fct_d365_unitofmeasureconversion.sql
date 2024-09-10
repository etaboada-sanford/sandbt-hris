-- conversion table for units - use in salesorderline etc to get kg
-- join to this table on upper(salesunit) as from_unit and KG as to_unit
-- and this table dim_d365_item_sk is null or matches the sales line dim_d365_item_sk 
-- where rnk = 1.  This is so if there is an item match that record is used over a standard conversion
-- e.g Item 30MSGHSH010 has a specifc conversion formula, and the std kg/lb formula, use the specific one

/*
example use

select salesid, linenum 
    , sl.itemid
    , currencycode
    , round(costprice, 6) costprice
    , round(lineamount,2) lineamount    -- in sales currency, total
    , round(priceunit, 3) priceunit
    , round(qtyordered, 3) qtyordered    -- in kg converted by D365 from salesqty 
    , round(salesprice, 2) salesprice
    , round(salesqty, 3) salesqty
    , salesunit
------------------------------------------------------------------    
    , coalesce(uom.conv_factor, uom2.conv_factor, 1) conv_factor

    -- check qtyordered - only when salesqty != qtyordered
    , case when salesqty != qtyordered then
        round(salesqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) 
        else qtyordered end
        as qty_chk
------------------------------------------------------------------    
    , round(remaininventfinancial, 3) remaininventfinancial
    , round(remaininventphysical, 3) remaininventphysical

from {{source('fno', 'salesline')}} sl
left join {{source('fno', 'inventtable')}} i on sl.itemid = i.itemid
    and upper(sl.dataareaid) = upper(i.dataareaid)
left join {{ref('dim_d365_item')}} itm on i.itemid = itm.itemid 
    and upper(i.dataareaid) = itm.item_dataareaid
------------------------------------------------------------------
-- UoM by Item first 
left join fct_d365_unitofmeasureconversion uom on 
    upper(sl.salesunit) = uom.from_unit
    and uom.to_unit = 'KG'
    and itm.dim_d365_item_sk = uom.dim_d365_item_sk
-- If no Item specific UoM 
left join fct_d365_unitofmeasureconversion uom2 on 
    upper(sl.salesunit) = uom2.from_unit
    and uom2.to_unit = 'KG'
    and uom2.dim_d365_item_sk is null
    and uom.dim_d365_item_sk is null
------------------------------------------------------------------

where 
    upper(salesunit)  in ('LB')
    and round(salesqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) != qtyordered

*/

with conv as (
    select 
        uomc.recid uomconversion_recid
        , uom_from.symbol from_unit
        , uom_to.symbol to_unit
        , uomc.numerator
        , uomc.denominator
        , uomc.factor   /* this is numeric(32,16) */
        /* must convert to num(32,16) to exactly match D365 calculations */
        , cast((cast(uomc.numerator as decimal(32, 16)) / cast(uomc.denominator as decimal(32, 16))) * uomc.factor as decimal(32, 16)) as conv_factor
        , itm.dim_d365_item_sk
        , itm.itemid
        , itm.item_dataareaid
    from {{source('fno', 'unitofmeasureconversion')}} uomc
    join {{source('fno', 'unitofmeasure')}} uom_from on uomc.fromunitofmeasure = uom_from.recid
    join {{source('fno', 'unitofmeasure')}} uom_to on uomc.tounitofmeasure = uom_to.recid
    left join {{source('fno', 'inventtable')}} i on uomc.product = i.product
    left join {{ref('dim_d365_item')}} itm on i.itemid = itm.itemid 
        and upper(i.dataareaid) = itm.item_dataareaid
    where 
        uomc.IsDelete is null
        and uom_from.IsDelete is null
        and uom_to.IsDelete is null
        /* only where item is valid */
        and (coalesce(uomc.product, 0) = 0 or itm.dim_d365_item_sk is not null)
)

, conv2 as (
    /* forward conversion */
    select 
        {{ dbt_utils.generate_surrogate_key(['uomconversion_recid'])}} as fact_d365_uomconversion_sk 
        , upper(from_unit) from_unit
        , upper(to_unit) to_unit
        , conv_factor
        , dim_d365_item_sk
        , itemid
        , item_dataareaid
    from conv
    /* reverse conversion */
    union all
    select 
        {{ dbt_utils.generate_surrogate_key(['uomconversion_recid * -1'])}} as fact_d365_uomconversion_sk
        , upper(to_unit) from_unit
        , upper(from_unit) to_unit
        , cast(cast(1 as decimal(32, 16)) / conv_factor as decimal(32, 16)) as conv_factor -- the reverse_conv_factor
        , dim_d365_item_sk
        , itemid
        , item_dataareaid
    from conv f
    /* where reverse not already exists */
    where not exists (
        select 1
        from conv r
        where upper(f.to_unit) = upper(r.from_unit)
            and upper(f.from_unit) = upper(r.to_unit)
            and (f.dim_d365_item_sk IS NULL AND r.dim_d365_item_sk IS NULL OR f.dim_d365_item_sk = r.dim_d365_item_sk)
    )
)

select fact_d365_uomconversion_sk 
    , from_unit
    , to_unit
    , conv_factor
    , dim_d365_item_sk
    , itemid
    , item_dataareaid
    , dense_rank() over(partition by from_unit, to_unit order by case when dim_d365_item_sk is not null then 1 else 2 end) as rnk
from conv2