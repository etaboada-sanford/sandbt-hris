/* 53440 Export Shipping - Air - (Consignment despatched as per original bookings as a % of total consigments)
 2023-07-12 smwood rewrote to include LOADS which give the modecode via dxc_transportationmethod for <i>some</i> orders
*/

with ctsort as (
    select distinct /* each SO can have multiple loads, each route can have multiple lines, only need the distinct values */
        st.[Id] as stg_d365_salesorder_freightby_1_sk
        , st.recid as salesorder_recid
        , st.salesid
        , case when s.modecode = 'OCEAN' then 1 else 0 end as s_freight_by_ocean
        , case when s.modecode = 'AIR' then 1 else 0 end as s_freight_by_air
        , case
            when s.modecode = 'TL' then 1 else 0 end as s_freight_by_tl

        , case when rts.modecode = 'OCEAN' then 1 else 0 end as rs_freight_by_ocean
        , case when rts.modecode = 'AIR' then 1 else 0 end as rs_freight_by_air
        , case
            when rts.modecode = 'TL' then 1 else 0 end as rs_freight_by_tl

        , case when cs.methodcode = 'OCEAN' then 1 else 0 end as cs_freight_by_ocean
        , case when cs.methodcode = 'AIR' then 1 else 0 end as cs_freight_by_air
        , case when cs.methodcode = 'TL' then 1 else 0 end as cs_freight_by_tl

        , case when wlt.dxc_transportationmethod = 'OCEAN' then 1 else 0 end as l_freight_by_ocean
        , case when wlt.dxc_transportationmethod = 'AIR' then 1 else 0 end as l_freight_by_air
        , case when wlt.dxc_transportationmethod = 'TL' then 1 else 0 end as l_freight_by_tl

        , upper(st.dataareaid) as dataareaid

        , st.[IsDelete]
        , st.versionnumber
        , st.sysrowversion
        from {{ source('fno', 'salestable') }} as st
            left join {{ source('fno', 'tmssalestable') }} as s
                on st.salesid = s.salesid
                    and s.[IsDelete] is null
            /* to get the actual route assigned to the order, not just ALL with the same routeconfigcode */
            left join {{ source('fno', 'whsloadtable') }} as wlt on s.salesid = wlt.ordernum and wlt.[IsDelete] is null
            /* where the order has a Route assigned */
            left join
                {{ source('fno', 'tmsroute') }}
                    as rt
                on s.routeconfigcode = rt.routeconfigcode and s.dataareaid = rt.dataareaid and rt.[IsDelete] is null
                    and wlt.routecode = rt.routecode
            left join {{ source('fno', 'tmsroutesegment') }} as rts on rt.routecode = rts.routecode and rt.dataareaid = rts.dataareaid and rts.[IsDelete] is null
            /* Carrier Service has methodcode AIR, SEA, TL
                        -- possible issue with NZC-CT (NZ Couriers) ALL shipments are marked with AIR as this is the only Service Code */
            left join
                {{ source('fno', 'tmscarrierservice') }}
                    as cs
                on s.carriercode = cs.carriercode
                    and s.carrierservicecode = cs.carrierservicecode
                    and cs.[IsDelete] is null
            {%- if is_incremental() %}
                and st.sysrowversion > {{ get_max_sysrowversion() }}
            {%- else %}
                and st.[IsDelete] is null
            {% endif %}
)

/* - not required as WHSLoadTable referenced in ctsort
        , ct_load as (
        select distinct
        s.recid salesorder_recid
        , s.salesid
        , upper(s.dataareaid) dataareaid
        , case when wlt.dxc_transportationmethod = 'OCEAN' then 1 else 0 end l_freight_by_ocean
        , case when wlt.dxc_transportationmethod = 'AIR' then 1 else 0 end l_freight_by_air
        , case when wlt.dxc_transportationmethod = 'TL' then 1 else 0 end l_freight_by_tl
        from {{ source('fno', 'whsloadtable') }} wlt
        join {{ source('fno', 'whsloadline') }} wll on wlt.loadid = wll.loadid
        join {{ source('fno', 'salestable') }} s on wll.ordernum = s.salesid
        where wlt.[IsDelete] is null
        and wll.[IsDelete] is null
        )

        -- Get all possible sales orders
        , ct_so as (
        select distinct
        salesorder_recid
        , salesid
        , dataareaid
        from ctsort
        union
        select distinct
        salesorder_recid
        , salesid
        , dataareaid
        from ct_load
        )
        */

select
    stg_d365_salesorder_freightby_1_sk
    , ctsort.salesorder_recid
    , ctsort.salesid
    , ctsort.dataareaid
    , case when sum(ctsort.s_freight_by_ocean) >= 1 then 1 else 0 end as s_freight_by_ocean
    , case when sum(ctsort.s_freight_by_air) >= 1 then 1 else 0 end as s_freight_by_air
    , case when sum(ctsort.s_freight_by_tl) >= 1 then 1 else 0 end as s_freight_by_tl
    , case when sum(ctsort.rs_freight_by_ocean) >= 1 then 1 else 0 end as rs_freight_by_ocean
    , case when sum(ctsort.rs_freight_by_air) >= 1 then 1 else 0 end as rs_freight_by_air
    , case when sum(ctsort.rs_freight_by_tl) >= 1 then 1 else 0 end as rs_freight_by_tl
    , case when sum(ctsort.cs_freight_by_ocean) >= 1 then 1 else 0 end as cs_freight_by_ocean
    , case when sum(ctsort.cs_freight_by_air) >= 1 then 1 else 0 end as cs_freight_by_air
    , case when sum(ctsort.cs_freight_by_tl) >= 1 then 1 else 0 end as cs_freight_by_tl
    , case when sum(ctsort.l_freight_by_ocean) >= 1 then 1 else 0 end as l_freight_by_ocean
    , case when sum(ctsort.l_freight_by_air) >= 1 then 1 else 0 end as l_freight_by_air
    , case when sum(ctsort.l_freight_by_tl) >= 1 then 1 else 0 end as l_freight_by_tl
    , ctsort.[IsDelete]
    , ctsort.versionnumber
    , ctsort.sysrowversion
from ctsort
group by
    ctsort.stg_d365_salesorder_freightby_1_sk
    , ctsort.salesorder_recid
    , ctsort.salesid
    , ctsort.dataareaid
    , ctsort.[IsDelete]
    , ctsort.versionnumber
    , ctsort.sysrowversion
