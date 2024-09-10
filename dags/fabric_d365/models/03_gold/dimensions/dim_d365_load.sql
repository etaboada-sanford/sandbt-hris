{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_load_sk']
) }}

with ld as (
    select
        wlt.[Id] as dim_d365_load_sk
        , wlt.recid as load_recid
        , wlt.destinationpostaladdress
        , wlt.originpostaladdress
        , wlt.loadid
        , wlt.loadreferencenum
        , wlt.loadstatus as loadstatusid
        , {{ translate_enum('els', 'wlt.loadstatus' ) }} as loadstatus
        , wlt.loadtemplateid


        /* , wt.TEU --Twenty Foot Equivalent Unit - 1 for a 20' container, 2 for a 40' */
        , case
            when left(wt.equipmentcode, 1) = '2' then 1
            when left(wt.equipmentcode, 1) = '4' then 2
        end as teu
        , wlt.loadpaysfreight
        , wlt.carriercode
        , wlt.carrierservicecode
        , wlt.accountnum
        , wlt.bookingnum
        /* , wlt.ordernum incorrect represntation of column can have Transfer Orders as well as Sales Orders */
        , st.salesid
        , wlt.carnumber
        , wlt.custvendref
        , wlt.destinationname
        , wlt.dxc_loadinglocationcode as loadinglocationcode
        , ld.description as loadinglocation
        , wlt.dxc_dischargelocationcode as dischargelocationcode
        , disch.description as dischargelocation
        , wlt.dxc_dispatchlocationcode as dispatchlocationcode
        , desp.description as dispatchlocation
        , wlt.dxc_destinationlocationcode as destinationlocationcode
        , dest.description as destinationlocation
        , wlt.dxc_transhipmentlocationcode as transhipmentlocationcode
        , trans.description as transhipmentlocation
        , wlt.vesselname
        , wlt.voyagenum
        , {{ translate_enum('eld', 'wlt.loaddirection' ) }} as loaddirection
        , wlt.dxc_vesselcode as vesselcode
        , wlt.tractornumber as feeder_vessel_name
        , wlt.carnumber as feeder_voyage_number
        , wlt.dxc_containernumber as containernumber
        , wlt.shippingcontainerid
        , wlt.dxc_sealnumber as sealnumber
        , wlt.dxc_noofpallets as noofpallets  /* count of LP's in dxc_pallets */
        , wlt.dxc_noofpacks as noofpacks      /* sum of WHSLoadLine.dxc_noofpacks */
        , wlt.dxc_pallets as pallets          /* string of LP's in form {"0000000001600073", "0000000001600080"} */
        , wlt.actualgrossweight
        , wlt.dxc_actualgrossweight
        , wlt.actualnetweight
        , wlt.loadnetweight
        , wlt.actualtareweight
        , wlt.dxc_transportationmethod as transportationmethod
        , {{ nullify_invalid_date('wlt.dxc_confirmedcedoarrivaldate') }} as confirmedcedoarrivaldate
        , {{ nullify_invalid_date('wlt.dxc_requestedcedoarrivaldate') }} as requestedcedoarrivaldate
        /* confirmeddischargeportdate = 'ETA Final destination' in sales reporting */
        , {{ nullify_invalid_date('wlt.dxc_confirmeddischargeportdate') }} as confirmeddischargeportdate
        /* eta = 'Estimated Departure - Feeder POA' in sales reporting */
        , {{ nullify_invalid_date('wlt.eta') }} as eta
        , wlteta.newzealandtime as eta_nzt
        , {{ nullify_invalid_date('wlt.etd') }} as etd
        , wltetd.newzealandtime as etd_nzt
        , {{ nullify_invalid_date('wlt.cutoffutcdatetime') }} as cutoffutcdatetime
        , wltcutoffutcdatetime.newzealandtime as cutoffdatetime_nzt
        , {{ nullify_invalid_date('wlt.lastupdateutcdatetime') }} as lastupdatedatetime
        , wltlastupdateutcdatetime.newzealandtime as lastupdatedatetime_nzt
        , {{ nullify_invalid_date('wlt.loadarrivalutcdatetime') }} as loadarrivaldatetime
        , wltloadarrivalutcdatetime.newzealandtime as loadarrivaldatetime_nzt
        , {{ nullify_invalid_date('wlt.loadschedshiputcdatetime') }} as loadschedshipdatetime
        , wltloadschedshiputcdatetime.newzealandtime as loadschedshipdatetime_nzt
        , {{ nullify_invalid_date('wlt.loadshipconfirmutcdatetime') }} as loadshipconfirmdatetime
        , wltloadshipconfirmutcdatetime.newzealandtime as loadshipconfirmdatetime_nzt
        /* sailutcdatetime = 'Estimated Departure - CEDO' in sales reporting */
        , {{ nullify_invalid_date('wlt.sailutcdatetime') }} as saildatetime
        , wltsailutcdatetime.newzealandtime as saildatetime_nzt

        , wlt.lateshipreasoncode
        , wlt.invalid
        , wlt.inventsiteid as siteid
        , s.name as site_name
        , wlt.inventlocationid as warehouseid
        , w.name as warehouse_name
        , upper(wlt.dataareaid) as load_dataareaid
        , wlt.[IsDelete]
        , wlt.versionnumber
        , wlt.sysrowversion

    from {{ source('fno', 'whsloadtable') }} as wlt
    left join {{ source('fno', 'inventlocation') }} as w on wlt.inventlocationid = w.inventlocationid and upper(wlt.dataareaid) = upper(w.dataareaid) and w.[IsDelete] is null and wlt.[IsDelete] is null
    left join {{ source('fno', 'inventsite') }} as s on wlt.inventsiteid = s.siteid and upper(wlt.dataareaid) = upper(s.dataareaid) and s.[IsDelete] is null and wlt.[IsDelete] is null
    cross apply dbo.f_get_enum_translation('whsloadtable', '1033') as els
    cross apply dbo.f_get_enum_translation('whsloadtable', '1033') as eld
    left join {{ source('fno', 'whsloadtemplate') }} as wt on wlt.loadtemplateid = wt.loadtemplateid and upper(wlt.dataareaid) = upper(wt.dataareaid) and wt.[IsDelete] is null
    left join {{ source('fno', 'intrastatport') }} as dest on wlt.dxc_destinationlocationcode = dest.portid and dest.[IsDelete] is null
    left join {{ source('fno', 'intrastatport') }} as desp on wlt.dxc_dispatchlocationcode = desp.portid and desp.[IsDelete] is null
    left join {{ source('fno', 'intrastatport') }} as ld on wlt.dxc_loadinglocationcode = ld.portid and ld.[IsDelete] is null
    left join {{ source('fno', 'intrastatport') }} as disch on wlt.dxc_dischargelocationcode = disch.portid and disch.[IsDelete] is null
    left join {{ source('fno', 'intrastatport') }} as trans on wlt.dxc_transhipmentlocationcode = trans.portid and trans.[IsDelete] is null

    /* To get actual SalesID */
    left join {{ source('fno', 'salestable') }} as st on wlt.ordernum = st.salesid and upper(wlt.dataareaid) = upper(st.dataareaid)
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.eta) as wlteta
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.etd) as wltetd
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.cutoffutcdatetime) as wltcutoffutcdatetime
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.lastupdateutcdatetime) as wltlastupdateutcdatetime
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.loadarrivalutcdatetime) as wltloadarrivalutcdatetime
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.loadschedshiputcdatetime) as wltloadschedshiputcdatetime
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.loadshipconfirmutcdatetime) as wltloadshipconfirmutcdatetime
    cross apply {{ this.database }}.dbo.f_convert_utc_to_nzt(wlt.sailutcdatetime) as wltsailutcdatetime
    {%- if is_incremental() %}
        where wlt.sysrowversion > {{ get_max_sysrowversion() }}
    {%- else %}
        where wlt.[IsDelete] is null
    {% endif %}

)

select
    dim_d365_load_sk
    , load_recid
    , destinationpostaladdress
    , originpostaladdress
    , loadid
    , loadreferencenum
    , loadstatusid
    , loadstatus
    , loadtemplateid
    , teu
    , loadpaysfreight
    , carriercode
    , carrierservicecode
    , accountnum
    , bookingnum
    , salesid
    , carnumber
    , custvendref
    , destinationname
    , loadinglocationcode
    , loadinglocation
    , dischargelocationcode
    , dischargelocation
    , dispatchlocationcode
    , dispatchlocation
    , destinationlocationcode
    , destinationlocation
    , transhipmentlocationcode
    , transhipmentlocation
    , vesselname
    , voyagenum
    , loaddirection
    , vesselcode
    , feeder_vessel_name
    , feeder_voyage_number
    , containernumber
    , shippingcontainerid
    , sealnumber
    , noofpallets
    , noofpacks
    , pallets
    , actualgrossweight
    , dxc_actualgrossweight
    , actualnetweight
    , loadnetweight
    , actualtareweight
    , transportationmethod
    , confirmedcedoarrivaldate
    , requestedcedoarrivaldate
    , confirmeddischargeportdate
    , confirmeddischargeportdate as eta_final_destination_date
    , eta
    , eta_nzt
    , eta_nzt as estimated_departure_feeder_poa
    , etd
    , etd_nzt
    , cutoffutcdatetime
    , cutoffdatetime_nzt
    , lastupdatedatetime
    , lastupdatedatetime_nzt
    , loadarrivaldatetime
    , loadarrivaldatetime_nzt
    , loadarrivaldatetime_nzt as cut_off_date_poa
    , loadschedshipdatetime
    , loadschedshipdatetime_nzt
    , loadshipconfirmdatetime
    , loadshipconfirmdatetime_nzt
    , saildatetime
    , saildatetime_nzt
    , saildatetime_nzt as estimated_departure_cedo
    , lateshipreasoncode
    , invalid
    , siteid
    , site_name
    , warehouseid
    , warehouse_name
    , load_dataareaid
    , [IsDelete]
    , versionnumber
    , sysrowversion
from ld
