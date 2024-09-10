{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_lea_contact_sk'],
    tags=["logisticselectronicaddress"]
) }}

select
    lea.[Id] as stg_d365_lea_contact_sk
    , p.recid as party_recid
    , lea.recid as contact_recid
    , lea.description as contact_description
    , lea.isprimary as contact_isprimary
    , lea.locator as locators
    , lea.locatorextension
    , lea.type
    , lea.electronicaddressroles as [role]
    , {{ translate_enum('enum', 'lea.type' ) }} as contacttype
    , lea.[IsDelete]
    , lea.versionnumber
    , lea.sysrowversion
from {{ source('fno', 'dirpartytable') }} as p
inner join {{ source('fno', 'dirpartylocation') }} as pl
    on p.recid = pl.party
inner join {{ source('fno', 'logisticslocation') }} as ll
    on pl.location = ll.recid
inner join {{ source('fno', 'logisticselectronicaddress') }} as lea
    on ll.recid = lea.location
cross apply dbo.f_get_enum_translation('logisticselectronicaddress', '1033') as enum
where {{ translate_enum('enum', 'lea.type' ) }} = 'Email address' and coalesce(lea.locator, '') != ''
    {%- if is_incremental() %}
        and lea.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
    and lea.[IsDelete] is null
{% endif %}
