{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_inventjournal_sk']
) }}

select
    ijt.[Id] as dim_d365_inventjournal_sk
    , ijt.recid as inventjournal_recid
    , ijt.journalid
    , ijn.journalnameid
    , {{ translate_enum('inventjournalname_enum', 'ijn.journaltype' ) }} as journaltype
    , ijt.description
    , ijt.posted
    /*
    , case
        when convert(date, ijt.posteddatetime) = '1900-01-01' then null
        when ijt.posteddatetimetzid = 37001 then ijtposteddatetime.NewZealandTime
        else ijt.posteddatetime
        end as posteddatetime
    */
    , ijtposteddatetime.newzealandtime as posteddatetime
    /* if user is removed from dirpersonuser the result will be null, so use the userid. Use D365BatchUser rather than username Enterprise */
    , {{ translate_enum('inventjournaltable_enum', 'ijt.workflowapprovalstatus' ) }} as workflowapprovalstatus
    , ijt.inventsiteid
    , ijt.inventlocationid
    , case when ijt.posteduserid = 'D365BatchUser' then 'D365BatchUser' else coalesce(p.name, ijt.posteduserid) end as postedby

    , upper(ijt.dataareaid) as inventjournal_dataareaid
    , ijt.[IsDelete]
    , ijt.versionnumber
    , ijt.sysrowversion

from {{ source('fno', 'inventjournaltable') }} as ijt
inner join {{ source('fno', 'inventjournalname') }} as ijn
    on ijt.journalnameid = ijn.journalnameid
        and ijt.dataareaid = ijn.dataareaid
left join {{ source('fno', 'dirpersonuser') }} as u on ijt.posteduserid = u.[user]
left join {{ ref('dim_d365_party') }} as p on u.personparty = p.party_recid
cross apply dbo.f_convert_utc_to_nzt(ijt.posteddatetime) as ijtposteddatetime
cross apply dbo.f_get_enum_translation('inventjournaltable', '1033') as inventjournaltable_enum
cross apply dbo.f_get_enum_translation('inventjournalname', '1033') as inventjournalname_enum
{%- if is_incremental() %}
    where ijt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where ijt.[IsDelete] is null
{% endif %}