{{ config(
    materialized = 'incremental', 
    unique_key = ['exchangerate_recid']
) }}

with ct1price as (
    select * from (
        select
            ip.itemid
            , id.inventsiteid
            , ip.activationdate
            , ip.priceunit
            , ip.priceqty
            , ip.pricecalcid
            , ip.pricetype
            , ip.price
            , ip.createddatetime
            , case when ip.priceunit in (0, 1) then ip.price else ip.price / ip.priceunit end as unitcost
            , upper(ip.dataareaid) as inventitemprice_dataareaid
            , ip.versionnumber
            , ip.sysrowversion
            /* must order by activation date DESC and recid DESC (created date is not unique enough) */
            , rank() over (
                partition by ip.dataareaid, id.inventsiteid, ip.itemid
                order by ip.dataareaid asc, id.inventsiteid asc, ip.activationdate desc, ip.recid desc
            ) as rnk

        from {{ source('fno', 'inventitemprice') }} as ip
        inner join {{ source('fno', 'inventdim') }} as id on ip.inventdimid = id.inventdimid
        where
            ip.[IsDelete] is null
            /* ONLY INCLUDE COST TYPE = 0 (type 1 = Purchase Price) */
            and ip.pricetype = 0
            and ip.activationdate <= convert(date, getdate())
            and id.configid is null
    ) as x
    where rnk = 1
)

select * from ct1price
