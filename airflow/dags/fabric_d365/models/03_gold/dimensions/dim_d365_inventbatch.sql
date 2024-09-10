{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_inventbatch_sk']
) }}

/*
    createddatetime is in UTC and needs converting to NZT
    ods_d365_inventbatch
    ods_d365_pdshistoryinventdisposition

    ctdt.newzealandtime as createddatetime_nzt

*/

with ct as (
    /* cr delim string of date, reason and details for dispostion*/
    select
        upper(phist.inventbatchid) as inventbatchid
        , phist.itemid
        , phist.[IsDelete]
        , phist.versionnumber
        , phist.sysrowversion
        , replace(replace(
            string_agg(concat(convert(date, ctdt.newzealandtime), '#', phist.newdispositioncode, '#', phist.dxc_inventdispositionreasoncode, '#', phist.dxc_batchdispositiondetails), '|')
            , '|', char(10)
        ), '#', char(9)) as dxc_batchdispositiondetails_history
    from {{ source('fno', 'pdshistoryinventdisposition') }} as phist
    cross apply dbo.f_convert_utc_to_nzt(phist.createddatetime) as ctdt
    where coalesce(phist.dxc_inventdispositionreasoncode, '') != ''
        or coalesce(phist.dxc_batchdispositiondetails, '') != ''
    group by
        upper(phist.inventbatchid)
        , phist.itemid
        , phist.[IsDelete]
        , phist.versionnumber
        , phist.sysrowversion
)

, ct1 as (
    /* most recent disposition record */
    select * from (
        select
            idh.itemid
            , idh.dxc_inventdispositionreasoncode as inventdispositionreasoncode
            , idh.dxc_batchdispositiondetails as batchdispositiondetails
            , idh.[IsDelete]
            , idh.versionnumber
            , idh.sysrowversion
            , upper(idh.inventbatchid) as inventbatchid
            , rank() over (partition by upper(idh.inventbatchid), idh.itemid order by ctdt.newzealandtime desc) as rnk
        from {{ source('fno', 'pdshistoryinventdisposition') }} as idh
        cross apply dbo.f_convert_utc_to_nzt(idh.createddatetime) as ctdt
        where coalesce(idh.dxc_inventdispositionreasoncode, '') != ''
            or coalesce(idh.dxc_batchdispositiondetails, '') != ''
    ) as x
    where rnk = 1
)

, cthist as (
    /* join history to latest disposition record */
    select
        ct1.*
        , case when ct.dxc_batchdispositiondetails_history = '' then null else ct.dxc_batchdispositiondetails_history end as batchdispositiondetails_history
    from ct1
    left join ct on ct1.inventbatchid = ct.inventbatchid and ct1.itemid = ct.itemid
)

/* TBC - if batch created with QCHOLD and no history - only present for migrated records */
, ctqcflag as (
    select
        upper(pdh.inventbatchid) as inventbatchid
        , pdh.itemid
        , pdh.[IsDelete]
        , pdh.versionnumber
        , pdh.sysrowversion
        /* where batch put on QC hold today or yesterday, when today sat, sun, or mon include back to friday in y'day */
        , max(case when convert(date, ctdt.newzealandtime) = convert(date, getdate()) then 1 else 0 end) as qchold_today
        , max(case
            when convert(date, ctdt.newzealandtime) = convert(date, dateadd(d, -1, getdate())) then 1
            when dt_today.day_abbrev = 'Sun' and convert(date, ctdt.newzealandtime) between convert(date, dateadd(d, -2, getdate())) and convert(date, dateadd(d, -1, getdate())) then 1
            when dt_today.day_abbrev = 'Mon' and convert(date, ctdt.newzealandtime) between convert(date, dateadd(d, -3, getdate())) and convert(date, dateadd(d, -1, getdate())) then 1
            else 0
        end) as qchold_yesterday

        , max(convert(date, ctdt.newzealandtime)) as qchold_date

    from {{ source('fno', 'pdshistoryinventdisposition') }} as pdh
    inner join {{ ref('dim_date') }} as dt_today on convert(date, getdate()) = dt_today.calendar_date
    cross apply dbo.f_convert_utc_to_nzt(pdh.createddatetime) as ctdt
    where
        pdh.[IsDelete] is null
        and (
            coalesce(pdh.dxc_inventdispositionreasoncode, '') != ''
            or coalesce(pdh.dxc_batchdispositiondetails, '') != ''
        )
        and pdh.newdispositioncode = 'QCHOLD'

    group by
        upper(pdh.inventbatchid)
        , pdh.itemid
        , pdh.[IsDelete]
        , pdh.versionnumber
        , pdh.sysrowversion
)

, ctib as (
    select
        ib.[Id] as dim_d365_inventbatch_sk
        , ib.recid as inventbatch_recid
        , ib.itemid
        , ib.pdsdispositioncode
        , ib.dxc_inventlocationid as inventlocationid
        , cthist.batchdispositiondetails_history
        , avf.pdsbatchattribvalue as farmno
        , avca.pdsbatchattribvalue as catchharvestarea  /* current status (blank, available or .... )  */
        , avlp.pdsbatchattribvalue as landingport
        /* change to same logic as used in the Transport Loadout Report */
        , avlc.pdsbatchattribvalue as linecageno

        , avm.pdsbatchattribvalue as manufacturerid
        , avp.pdsbatchattribvalue as productionsite
        , avvn.pdsbatchattribvalue as vesselname
        , avvnm.pdsbatchattribvalue as vesselnumber
        , ctqcflag.qchold_date
        , ib.partition

        /* change to same logic as used in the Transport Loadout Report */
        , upper(ib.inventbatchid) as inventbatchid

        /* dates for these are stored as integers past 1900-01-01
        Catch/HarvestDateEnd
        Catch/HarvestDateSta
        FreezingDate
        DateOfHarvest
        DateOfLanding
        */
        , convert(date, ib.proddate) as proddate
        , convert(date, ib.pdsbestbeforedate) as pdsbestbeforedate
        , convert(date, ib.pdsfinishedgoodsdatetested) as pdsfinishedgoodsdatetested
        , convert(date, ib.pdsshelfadvicedate) as pdsshelfadvicedate
        , convert(date, ib.pdsvendbatchdate) as pdsvendbatchdate

        , convert(date, ib.dxc_edecproductionstartdate)
            as productionstartdate
        , case when coalesce(convert(date, ib.dxc_edecproductionenddate), '1900-01-01') = '1900-01-01' then convert(date, ib.proddate) else convert(date, ib.dxc_edecproductionenddate) end
            as productionenddate
        , upper(ib.dataareaid) as inventbatch_dataareaid
        , case when coalesce(ib.pdsdispositioncode, '') in ('', 'Available') then null else cthist.inventdispositionreasoncode end as inventdispositionreasoncode
        , case when coalesce(ib.pdsdispositioncode, '') in ('', 'Available') then null else cthist.batchdispositiondetails end as batchdispositiondetails
        , case
            when coalesce(convert(date, ib.proddate), '1900-01-01') not in ('1900-01-01', '2009-01-01')
                and it.pdsshelflife > 0
                and coalesce(convert(date, ib.expdate), '1900-01-01') = '1900-01-01'
                then convert(date, dateadd(day, it.pdsshelflife, ib.proddate))
            else convert(date, ib.expdate)
        end as expdate
        , case when coalesce(avce.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, convert(int, avce.pdsbatchattribvalue), '1900-01-01')) end as catchharvestdateend
        , case when coalesce(avcs.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, convert(int, avcs.pdsbatchattribvalue), '1900-01-01')) end as catchharvestdatestart
        , case when coalesce(avdh.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, convert(int, avdh.pdsbatchattribvalue), '1900-01-01')) end as dateofharvest
        , case when coalesce(avdl.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, convert(int, avdl.pdsbatchattribvalue), '1900-01-01')) end as dateoflanding
        , case when coalesce(avfd.pdsbatchattribvalue, 0) = 0 then null else convert(date, dateadd(d, convert(int, avfd.pdsbatchattribvalue), '1900-01-01')) end as freezingdate
        , upper(avt.pdsbatchattribvalue) as tripno
        , coalesce(ctqcflag.qchold_today, 0) as qchold_today
        , coalesce(ctqcflag.qchold_yesterday, 0) as qchold_yesterday

        , ib.[IsDelete]
        , ib.versionnumber
        , ib.sysrowversion

    from {{ source('fno', 'inventbatch') }} as ib
    left join cthist
        on upper(ib.inventbatchid) = cthist.inventbatchid
            and ib.itemid = cthist.itemid
    left join {{ source('fno', 'inventtable') }} as it
        on ib.itemid = it.itemid
            and upper(ib.dataareaid) = upper(it.dataareaid)
            and it.[IsDelete] is null

    /* include all likely attributes used
        Catch/HarvestDateEnd
        Catch/HarvestDateSta
        CatchHarvestArea
        DateOfHarvest
        DateOfLanding
        FarmNo
        FreezingDate
        LandingPort
        LineCageNo
        ManufacturingId
        ProductionSite
        TripNo
        VesselName
        VesselNumber
        */

    left join {{ source('fno', 'pdsbatchattributes') }} as avce
        on upper(ib.dataareaid) = upper(avce.dataareaid)
            and upper(avce.pdsbatchattribid) = upper('Catch/HarvestDateEnd')
            and upper(ib.inventbatchid) = upper(avce.inventbatchid)
            and ib.itemid = avce.itemid
            and avce.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avcs
        on upper(ib.dataareaid) = upper(avcs.dataareaid)
            and upper(avcs.pdsbatchattribid) = upper('Catch/HarvestDateSta')
            and upper(ib.inventbatchid) = upper(avcs.inventbatchid)
            and ib.itemid = avcs.itemid
            and avcs.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avca
        on upper(ib.dataareaid) = upper(avca.dataareaid)
            and upper(avca.pdsbatchattribid) = upper('CatchHarvestArea')
            and upper(ib.inventbatchid) = upper(avca.inventbatchid)
            and ib.itemid = avca.itemid
            and avca.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avdh
        on upper(ib.dataareaid) = upper(avdh.dataareaid)
            and upper(avdh.pdsbatchattribid) = upper('DateOfHarvest')
            and upper(ib.inventbatchid) = upper(avdh.inventbatchid)
            and ib.itemid = avdh.itemid
            and avdh.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avdl
        on upper(ib.dataareaid) = upper(avdl.dataareaid)
            and upper(avdl.pdsbatchattribid) = upper('DateOfLanding')
            and upper(ib.inventbatchid) = upper(avdl.inventbatchid)
            and ib.itemid = avdl.itemid
            and avdl.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avf
        on upper(ib.dataareaid) = upper(avf.dataareaid)
            and upper(avf.pdsbatchattribid) = upper('FarmNo')
            and upper(ib.inventbatchid) = upper(avf.inventbatchid)
            and ib.itemid = avf.itemid
            and avf.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avfd
        on upper(ib.dataareaid) = upper(avfd.dataareaid)
            and upper(avfd.pdsbatchattribid) = upper('FreezingDate')
            and upper(ib.inventbatchid) = upper(avfd.inventbatchid)
            and ib.itemid = avfd.itemid
            and avfd.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avlp
        on upper(ib.dataareaid) = upper(avlp.dataareaid)
            and upper(avlp.pdsbatchattribid) = upper('LandingPort')
            and upper(ib.inventbatchid) = upper(avlp.inventbatchid)
            and ib.itemid = avlp.itemid
            and avlp.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avlc
        on upper(ib.dataareaid) = upper(avlc.dataareaid)
            and upper(avlc.pdsbatchattribid) = upper('LineCageNo')
            and upper(ib.inventbatchid) = upper(avlc.inventbatchid)
            and ib.itemid = avlc.itemid
            and avlc.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avm
        on upper(ib.dataareaid) = upper(avm.dataareaid)
            and upper(avm.pdsbatchattribid) = upper('ManufacturingId')
            and upper(ib.inventbatchid) = upper(avm.inventbatchid)
            and ib.itemid = avm.itemid
            and avm.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avp
        on upper(ib.dataareaid) = upper(avp.dataareaid)
            and upper(avp.pdsbatchattribid) = upper('ProductionSite')
            and upper(ib.inventbatchid) = upper(avp.inventbatchid)
            and ib.itemid = avp.itemid
            and avp.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avt
        on upper(ib.dataareaid) = upper(avt.dataareaid)
            and upper(avt.pdsbatchattribid) = upper('TripNo')
            and upper(ib.inventbatchid) = upper(avt.inventbatchid)
            and ib.itemid = avt.itemid
            and avt.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avvn
        on upper(ib.dataareaid) = upper(avvn.dataareaid)
            and upper(avvn.pdsbatchattribid) = upper('VesselName')
            and upper(ib.inventbatchid) = upper(avvn.inventbatchid)
            and ib.itemid = avvn.itemid
            and avvn.[IsDelete] is null

    left join {{ source('fno', 'pdsbatchattributes') }} as avvnm
        on upper(ib.dataareaid) = upper(avvnm.dataareaid)
            and upper(avvnm.pdsbatchattribid) = upper('VesselNumber')
            and upper(ib.inventbatchid) = upper(avvnm.inventbatchid)
            and ib.itemid = avvnm.itemid
            and avvnm.[IsDelete] is null

    left join ctqcflag
        on upper(ib.inventbatchid) = ctqcflag.inventbatchid
            and ib.itemid = ctqcflag.itemid
    {%- if is_incremental() %}
        where ib.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where ib.[IsDelete] is null
    {% endif %}
)

/* Approx 15K batch records do not have Trip details, but the InventBatchID is a TripID */
, ct_missing_trip as (
    select distinct
        ctib.dim_d365_inventbatch_sk
        , t.mserp_tripid as tripid
    from ctib
    /* must ref the ODS as stg_dim_d365_triporder refs stg_dim_d365_inventbatch */
    inner join
        {{ source('mserp', 'dxc_triporder') }}
            as t
        on ctib.inventbatch_dataareaid = upper(t.mserp_dataareaid)
            and ctib.inventbatchid = t.mserp_tripid
    where ctib.tripno is null
)

, ctfinal as (
    select
        ctib.dim_d365_inventbatch_sk
        , ctib.inventbatch_recid
        , ctib.inventbatchid
        , ctib.itemid
        , ctib.inventbatch_dataareaid
        , ctib.inventlocationid
        , ctib.partition
        , ctib.inventdispositionreasoncode
        , ctib.batchdispositiondetails
        , ctib.batchdispositiondetails_history
        , ctib.catchharvestdateend
        , ctib.catchharvestdatestart
        , ctib.catchharvestarea
        , ctib.dateofharvest
        , ctib.dateoflanding
        , ctib.farmno
        , ctib.freezingdate
        , ctib.landingport
        , ctib.linecageno

        , ctib.manufacturerid
        , ctib.productionsite
        , ctib.vesselname
        , ctib.vesselnumber
        , ctib.qchold_today
        , ctib.qchold_yesterday
        , ctib.qchold_date
        , ctib.[IsDelete]
        , ctib.versionnumber
        , ctib.sysrowversion
        , {{ nullify_invalid_date('ctib.proddate') }} as proddate
        , {{ nullify_invalid_date('ctib.pdsbestbeforedate') }} as pdsbestbeforedate
        , {{ nullify_invalid_date('ctib.pdsfinishedgoodsdatetested') }} as pdsfinishedgoodsdatetested
        , {{ nullify_invalid_date('ctib.pdsshelfadvicedate') }} as pdsshelfadvicedate
        , {{ nullify_invalid_date('ctib.pdsvendbatchdate') }} as pdsvendbatchdate
        , {{ nullify_invalid_date('ctib.pdsdispositioncode') }} as pdsdispositioncode
        , {{ nullify_invalid_date('ctib.productionstartdate') }} as productionstartdate
        , {{ nullify_invalid_date('ctib.productionenddate') }} as productionenddate
        , {{ nullify_invalid_date('ctib.expdate') }} as expdate
        , coalesce(ctib.tripno, ct_missing_trip.tripid) as tripno
    from ctib as ctib
    left join ct_missing_trip as ct_missing_trip on ctib.dim_d365_inventbatch_sk = ct_missing_trip.dim_d365_inventbatch_sk
)

select * from ctfinal
