with base as (
    select
        ip.recid
        , ip.itemid
        , ip.activationdate
        , ip.createddatetime
        , ip.inventdimid
        , ip.versionid
        , ip.price
        , ip.priceunit
        , ip.unitid
        , ip.dataareaid
        , id.inventsiteid as siteid
        , rank() over (partition by ip.itemid order by ip.createddatetime desc, ip.activationdate desc) as rnk

    from {{ source('fno', 'inventitemprice') }} as ip
    inner join {{ source('fno', 'inventdim') }} as id
        on
            ip.inventdimid = id.inventdimid
            and upper(ip.dataareaid) = upper(id.dataareaid)
    where
        id.inventsiteid = 'TRANSIT'
        and (ip.itemid like '10%' or ip.itemid like '20%' or ip.itemid like '30%')
)

, ctstd as (
    select x.*
    from base as x
    where x.rnk = 1
)

select
    recid
    , itemid
    , activationdate
    , createddatetime
    , inventdimid
    , versionid
    , round(price, 2) as standard_cost
    , round(priceunit, 3) as priceunit
    , upper(unitid) as unitid
    , upper(dataareaid) as item_standardcost_dataareaid
from ctstd
