{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_contact_sk']
) }}

select
    lea.[Id] as dim_d365_contact_sk
    , lea.recid as contact_recid
    , lea.countryregioncode
    , lea.description
    , lea.location
    , lea.locator
    , lea.locatorextension
    , lea.electronicaddressroles
    , lea.isprimary
    , lea.ismobilephone
    , pl.party as party_recid
    , {{ translate_enum('logisticselectronicaddress_enum', 'lea.type' ) }} as logisticselectronicaddress_methodtype
    , lea.[IsDelete]
    , lea.versionnumber
    , lea.sysrowversion
from {{ source('fno', 'logisticselectronicaddress') }} as lea
left join {{ source('fno', 'logisticslocation') }} as ll on lea.location = ll.recid
left join {{ source('fno', 'dirpartylocation') }} as pl on ll.recid = pl.location
cross apply dbo.f_get_enum_translation('logisticselectronicaddress', '1033') as logisticselectronicaddress_enum
{%- if is_incremental() %}
    where lea.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where lea.[IsDelete] is null
{% endif %}
