with ct as (
    select
        *
        , right(allocationid, (len(allocationid) - 3)) as allocation
    from {{ source('fno', 'forecastitemallocationline') }}
    where [IsDelete] is null
)

, ct2 as (
    select
        *
        , itemid as itemiddd
        , rank() over (partition by itemid, allocation order by recid) as allocation_rank
    from ct
)

, ct3 as (
    select
        *
        , rank() over (partition by itemiddd order by allocation_rank, recid) as allocation_rank_unique
    from ct2
)

, ct4 as (
    select *
    from ct3
)

, ct5 as (
    select
        ct4.allocation_rank
        , ct4.allocation_rank_unique
        , ct4.itemid
        , ct4.allocation
        , ct4.allocationid
        , row_number() over (partition by 0 order by ct4.recid) as item_allocation_id
        , case
            when left(ct4.allocation, 3) in ('SAM', 'MSG') then ct4.allocationid
            /*
            when i.dxc_speciescode in ('ATO', 'BAS','HAP','HPB','ORH','SCI') then concat('TIM',left(ct4.allocation,3))
            else concat('SAN',left(ct4.allocation,3))

            2024-07-11 discussion with Jason K - SAN & TIM require full allocation, not just first 3 chars. Devops #58737
            */
            when i.dxc_speciescode in ('ATO', 'BAS', 'HAP', 'HPB', 'ORH', 'SCI') then concat('TIM', ct4.allocation)
            else concat('SAN', ct4.allocation)
        end as sales_allocation
        , upper(ct4.dataareaid) as item_allocation_dataarea

    from ct4
    inner join {{ source('fno', 'inventtable') }}
        as i on ct4.itemid = i.itemid
        and upper(ct4.dataareaid) = upper(i.dataareaid)
        and i.[IsDelete] is null
)

select
    {{ dbt_utils.generate_surrogate_key(['item_allocation_id']) }} as dim_d365_item_allocation_sk
    , *
from ct5
