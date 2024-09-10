{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_lea_phone_sk'],
    tags=["logisticselectronicaddress"]
) }}

select
    a.[Id] as stg_d365_lea_phone_sk
    , a.recid
    , a.locator as primary_ph
    , a.description as primary_ph_desc
    , a.locatorextension as primary_ph_ext
    , a.electronicaddressroles as primary_ph_roles
    , a.[IsDelete]
    , a.versionnumber
    , a.sysrowversion
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactphone
where a.locator is not null
{%- if is_incremental() %}
    and a.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    and a.[IsDelete] is null
{% endif %}
