{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_purchaserequisition_sk']
) }}

select
    pt.[Id] as dim_d365_purchaserequisition_sk
    , pt.recid as purchaserequisition_recid

    , pt.purchreqid
    , pt.purchreqname
    , sdat.newzealandtime as submitteddatetime_nzt
    , pt.createdby
    , pt.submittedby
    /* all valid dates have 37001 as timezone id = GMT */
    , p.name as originator

    , pt.purchreqtype as purchreqtypeid
    , {{ translate_enum('eprt', 'pt.purchreqtype' ) }} as purchreqtype

    , pt.requisitionpurpose as requisitionpurposeid
    , {{ translate_enum('eprp', 'pt.requisitionpurpose' ) }} as requisitionpurpose

    , pt.requisitionstatus as requisitionstatusid
    , {{ translate_enum('eprs', 'pt.requisitionstatus' ) }} as requisitionstatus

    , pt.projid
    , pt.partition
    , cast(pt.requireddate as date) as requesteddate
    , cast(pt.transdate as date) as accountingdate

    , case when cast(pt.submitteddatetime as date) = '1900-01-01' then null else pt.submitteddatetime end as submitteddatetime
    , upper(co.dataarea) as buyinglegalentity_dataareaid
    , pt.versionnumber
    , pt.sysrowversion

from {{ source('fno', 'purchreqtable') }} as pt
cross apply dbo.f_convert_utc_to_nzt(pt.submitteddatetime) as sdat

cross apply dbo.f_get_enum_translation('purchreqtable', '1033') as eprt
cross apply dbo.f_get_enum_translation('purchreqtable', '1033') as eprp
cross apply dbo.f_get_enum_translation('purchreqtable', '1033') as eprs

left join {{ source('fno', 'companyinfo') }} as co on pt.companyinfodefault = co.recid
left join {{ source('fno', 'hcmworker') }} as h on pt.originator = h.recid
left join {{ ref('dim_d365_party') }} as p on h.person = p.party_recid
{%- if is_incremental() %}
    where pt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pt.[IsDelete] is null
{% endif %}
