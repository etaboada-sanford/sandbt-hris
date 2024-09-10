{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_party_sk']
) }}

select
    pt.[Id] as dim_d365_party_sk
    , pt.recid as party_recid
    , vpt.isactive
    , comp.coregnum
    , pt.partynumber
    , pt.name
    , pt.namealias
    , vpt.namesequence
    , vpt.namecontrol
    , pt.primarycontactphone
    , pt.primaryaddresslocation
    , vpt.numberofemployees
    , vpt.dataarea as party_dataareaid
    , pt.createddatetime
    , pt.createdby
    , pt.modifieddatetime
    , pt.modifiedby
    , lea_ph.locator as primary_ph

    , lea_ph.description as primary_ph_desc
    , lea_ph.locatorextension as primary_ph_ext
    , lea_ph.electronicaddressroles as primary_ph_roles
    , lea_f.locator as primary_fax

    , lea_f.description as primary_fax_desc
    , lea_f.locatorextension as primary_fax_ext
    , lea_f.electronicaddressroles as primary_fax_roles
    , lea_e.locator as primary_email

    , lea_e.description as primary_email_desc
    , lea_e.electronicaddressroles as primary_email_roles
    , pt.partition

    , null as [IsDelete]
    , coalesce(vpt.orgnumber, vpt.professionalsuffix) as orgnumber
    , pt.versionnumber
    , pt.sysrowversion

from {{ source('fno', 'dirpartytable') }} as pt
inner join {{ source('fno', 'vw_dirpartytable') }} as vpt on pt.recid = vpt.recid
left join {{ source('fno', 'companyinfo') }} as comp on pt.recid = comp.recid
left join {{ source('fno', 'logisticselectronicaddress') }} as lea_ph on pt.primarycontactphone = lea_ph.recid
left join {{ source('fno', 'logisticselectronicaddress') }} as lea_f on pt.primarycontactfax = lea_f.recid
left join {{ source('fno', 'logisticselectronicaddress') }} as lea_e on pt.primarycontactemail = lea_e.recid
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
