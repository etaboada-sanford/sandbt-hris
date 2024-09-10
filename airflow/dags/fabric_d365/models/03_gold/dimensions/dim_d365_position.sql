{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_position_sk']
) }}

select
    p.[Id] as dim_d365_position_sk
    , p.recid as position_recid
    , pd.recid as positiondetail_recid
    , pd.job as job_recid

    , p.positionid
    , pd.description
    , j.jobid
    , t.titleid
    , ou.omoperatingunitnumber as businessunit

    , pt.typeid as positiontypeid
    , pt.description as positiontype

    , pdvalidfrom.newzealandtime as validfrom_nzt
    , pdvalidto.newzealandtime as validto_nzt
    , p.partition

    , p.recversion
    , p.[IsDelete]
    , case when convert(date, pdvalidfrom.newzealandtime) <= convert(date, getdate()) and convert(date, pdvalidto.newzealandtime) > convert(date, getdate()) then 1 else 0 end as isvalid

    , p.versionnumber
    , p.sysrowversion
from {{ source('fno', 'hcmposition') }} as p
inner join {{ source('fno', 'hcmpositiondetail') }} as pd on p.recid = pd.position and pd.[IsDelete] is null
inner join {{ source('fno', 'hcmpositiontype') }} as pt on pd.positiontype = pt.recid and pt.[IsDelete] is null
left join {{ source('fno', 'hcmjob') }} as j on pd.job = j.recid and j.[IsDelete] is null
left join {{ source('fno', 'hcmtitle') }} as t on pd.title = t.recid and t.[IsDelete] is null
left join {{ source('fno', 'omoperatingunit') }} as ou on pd.department = ou.recid and ou.[IsDelete] is null
cross apply dbo.f_convert_utc_to_nzt(pd.validfrom) as pdvalidfrom
cross apply dbo.f_convert_utc_to_nzt(pd.validto) as pdvalidto
{%- if is_incremental() %}
    where p.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where p.[IsDelete] is null
{% endif %}
