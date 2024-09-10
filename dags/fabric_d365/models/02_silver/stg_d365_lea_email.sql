{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_lea_email_sk'],
    tags=["logisticselectronicaddress"]
) }}

select
    a.[Id] as stg_d365_lea_email_sk
    , a.recid
    , a.locator as primary_email
    , a.description as primary_email_desc
    , a.electronicaddressroles as primary_email_roles
    , a.[IsDelete]
    , a.versionnumber
    , a.sysrowversion
from {{ source('fno', 'logisticselectronicaddress') }} as a
inner join {{ source('fno', 'dirpartytable') }} as b on a.recid = b.primarycontactemail
where a.locator is not null
{%- if is_incremental() %}
    and a.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    and a.[IsDelete] is null
{% endif %}
