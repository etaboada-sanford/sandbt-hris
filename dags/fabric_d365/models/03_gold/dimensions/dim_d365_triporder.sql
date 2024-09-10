{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_triporder_sk']
) }}

/* Trips were not migrated from NAV, but we have SoH & Production in D365 for non-existant trips, so Prod Vol reporting does not assign Trip
    add in NAV trips where D365 batch references NAV Trip, that is not in D365
*/
with trip_d365 as (
    select
        dto.mserp_dxc_triporderentityid as dim_d365_triporder_sk
        , dto.mserp_dxc_triporderentityid as triporder_recid
        , dto.mserp_tripid as tripid
        , dto.mserp_description as description
        , dto.mserp_triptypeid as triptypeid
        , dto.mserp_tripdocketnumber as tripdocketnumber
        , dto.mserp_vesselid as vesselid
        , dto.mserp_vesselname as vesselname
        , dto.mserp_vendaccount as vendaccount
        , v.name as vendorname

        , dto.mserp_departureportid as departureportid
        , dto.mserp_landingportid as landingportid

        , dto.mserp_transferid as transferid
        , dto.mserp_transferoffloadwarehouse as transferoffloadwarehouse

        , dto.mserp_purchid as purchid
        , dto.mserp_projid as projid

        , dto.mserp_faoareaid as faoareaid
        , dto.mserp_catchharvestareaid as catchharvestareaid
        , dto.mserp_farmlinecageid as farmlinecageid
        , dto.mserp_musselcropid as musselcropid
        , dto.mserp_yeartripnum as yeartripnum

        , flc.mserp_description as farmlinecage
        , dto.mserp_inventsiteid as inventsiteid
        , dto.mserp_inventlocationid as inventlocationid
        , null as [IsDelete]
        , convert(date, dto.mserp_schedstartdate) as schedstartdate
        , convert(date, dto.mserp_schedenddate) as schedenddate

        , case when convert(date, dto.mserp_actualstartdate) = '1900-01-01' then null else convert(date, dto.mserp_actualstartdate) end as actualstartdate
        , case when convert(date, dto.mserp_actualenddate) = '1900-01-01' then null else convert(date, dto.mserp_actualenddate) end as actualenddate

        , case when convert(date, dto.mserp_landingdate) = '1900-01-01' then null else convert(date, dto.mserp_landingdate) end as landingdate
        , upper(dto.mserp_farmid) as farmid

        , flc.mserp_vendor1percent / 100 as vendor1percent
        , upper(dto.mserp_dataareaid) as triporder_dataareaid
        , 0 as versionnumber
        , 0 as sysrowversion
    from {{ source('mserp', 'dxc_triporder') }} as dto
    left join {{ ref('dim_d365_vendor') }} as v on dto.mserp_vendaccount = v.accountnum and upper(dto.mserp_dataareaid) = v.vendor_dataareaid
    left join {{ source('mserp', 'dxc_farmlinecage') }} as flc
        on
            upper(dto.mserp_farmid) = upper(flc.mserp_farmid)
            and upper(dto.mserp_farmlinecageid) = upper(flc.mserp_farmlinecageid)
            and dto.mserp_catchharvestareaid = flc.mserp_catchharvestareaid
            and flc.[IsDelete] is null
    {%- if is_incremental() %}
        where dto.sysrowversion > {{ get_max_sysrowversion() }}
    {%- else %}
        where dto.[IsDelete] is null
    {% endif %}
)

, trip_nav as (
    select
        nav_trip.dim_fishing_trip_key
        , nav_trip.fishing_trip_no
        , convert(date, nav_trip.landing_date) as landingdate
        , convert(date, nav_trip.starting_date_of_trip) as actualstartdate
        , convert(date, nav_trip.end_date_of_trip) as actualenddate

    from {{ source('nav', 'dim_fishing_trip') }} as nav_trip
    where nav_trip.is_deleted = 0
        and exists (
            select 1 as val from {{ ref('dim_d365_inventbatch') }} as ib
            where ib.inventbatch_dataareaid = 'SANF'
                and nav_trip.fishing_trip_no = ib.tripno
                and ib.tripno != ''
        )
        and not exists (
            select 1 as val from trip_d365
            where nav_trip.fishing_trip_no = trip_d365.tripid
        )
)

select *
from trip_d365

union all

select
    {{ dbt_utils.generate_surrogate_key(['dim_fishing_trip_key *-1']) }} as dim_d365_triporder_sk
    , dim_fishing_trip_key * -1 as triporder_recid
    , fishing_trip_no as tripid
    , 'NAV TRIP not migrated' as description
    , null as triptypeid
    , null as tripdocketnumber
    , null as vesselid
    , null as vesselname
    , null as vendaccount
    , null as vendorname

    , null as departureportid
    , null as landingportid

    , null as transferid
    , null as transferoffloadwarehouse

    , null as purchid
    , null as projid

    , actualstartdate as schedstartdate
    , actualenddate as schedenddate
    , actualstartdate
    , actualenddate
    , landingdate

    , null as faoareaid
    , null as catchharvestareaid
    , null as farmid
    , null as farmlinecageid
    , null as musselcropid
    , null as yeartripnum

    , null as vendor1percent
    , null as farmlinecage

    , null as inventsiteid
    , null as inventlocationid

    , 'SANF' as triporder_dataareaid
    , null as [IsDelete]
    , 0 as versionnumber
    , 0 as sysrowversion
from trip_nav
