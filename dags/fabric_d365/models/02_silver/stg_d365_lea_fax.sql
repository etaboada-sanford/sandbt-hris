{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_lea_fax_sk'],
    tags=["logisticselectronicaddress"]
) }}

select
    a.[Id] as stg_d365_lea_fax_sk
    , a.recid
    , a.locator as primary_fax
    , a.description as primary_fax_desc
    , a.locatorextension as primary_fax_ext
    , a.electronicaddressroles as primary_fax_roles
    , a.[IsDelete]
    , a.versionnumber
    , a.sysrowversion
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactfax
where a.locator is not null
{%- if is_incremental() %}
    and a.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    and a.[IsDelete] is null
{% endif %}
