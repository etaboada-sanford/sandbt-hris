{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_shipment_sk']
) }}

with utcbasedata as (
    select
        wst.[Id] as dim_d365_shipment_sk
        , wst.recid as shipment_recid
        , wst.shipmentid
        , wst.shipmentstatus as shipmentstatusid
        , {{ translate_enum('ess', 'wst.shipmentstatus' ) }} as shipmentstatus
        , wst.accountnum
        , wst.address
        , ad.city
        , wst.carriercode
        , wst.carrierservicecode
        , wst.customerref
        , wst.deliveryname
        , wst.dlvtermid

        , wst.loadid
        , wst.ordernum
        , wst.modecode
        , {{ translate_enum('eld', 'wst.loaddirection' ) }} as loaddirection
        , wst.inventsiteid as siteid

        , s.name as site_name
        , wst.inventlocationid as warehouseid
        , w.name as warehouse_name

        , wst.[IsDelete]
        , upper(ad.countryregionid) as countryregionid
        , {{ nullify_invalid_date('wst.dropoffutcdatetime') }} as dropoffdatetime
        , {{ nullify_invalid_date('wst.shipconfirmutcdatetime') }} as shipconfirmdatetime
        , {{ nullify_invalid_date('wst.shipmentarrivalutcdatetime') }} as shipmentarrivaldatetime
        , upper(wst.dataareaid) as shipment_dataareaid
        , wst.versionnumber
        , wst.sysrowversion

    from {{ source('fno', 'whsshipmenttable') }} as wst
    left join {{ source('fno', 'inventlocation') }} as w on wst.inventlocationid = w.inventlocationid and upper(wst.dataareaid) = upper(w.dataareaid)
    left join {{ source('fno', 'inventsite') }} as s on wst.inventsiteid = s.siteid and upper(wst.dataareaid) = upper(s.dataareaid)
    cross apply dbo.f_get_enum_translation('whsshipmenttable', '1033') as ess
    cross apply dbo.f_get_enum_translation('whsshipmenttable', '1033') as eld
    left join {{ source('fno', 'logisticspostaladdress') }} as ad on wst.deliverypostaladdress = ad.recid and ad.[IsDelete] is null

    {%- if is_incremental() %}
        where wst.sysrowversion > {{ get_max_sysrowversion() }}
    {%- else %}
        where wst.[IsDelete] is null
    {% endif %}
)

select
    ut.*
    , dropoff.newzealandtime as dropoffdatetime_nzt
    , shipconfirm.newzealandtime as shipconfirmdatetime_nzt
    , shipmentarrival.newzealandtime as shipmentarrivaldatetime_nzt
from utcbasedata as ut
cross apply dbo.f_convert_utc_to_nzt(ut.dropoffdatetime) as dropoff
cross apply dbo.f_convert_utc_to_nzt(ut.shipconfirmdatetime) as shipconfirm
cross apply dbo.f_convert_utc_to_nzt(ut.shipmentarrivaldatetime) as shipmentarrival
