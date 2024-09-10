{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_project_sk']
) }}

select
    pr.[Id] as dim_d365_project_sk
    , pr.recid as project_recid
    , pr.projid
    , pr.custaccount
    , pr.defaultdimension
    , fd.d1_businessunitvalue as businessunit
    , fd.d1_businessunit_name as businessunit_name
    , pr.deliverylocation
    , pr.dlvname
    , pr.jobid
    , pr.name
    , pr.parentid
    , pr.projgroupid
    , pg.projgroup_desc
    , pg.projgrouptype
    , pr.projinvoiceprojid
    , pr.projledgerposting
    , pr.projlinepropertysearch
    , pr.projpricegroup
    , pr.psaforecastmodelidexternal
    , pr.psaschedcalendarid
    , null as sortingid
    , null as sortingid2_
    , null as sortingid3_
    , {{ translate_enum('e', 'pr.status' ) }} as project_status
    , pr.taxgroupid
    , pr.type
    , pr.validateprojcategory
    , pt.name as project_responsible
    , pr.dxc_salmonunit as salmonunit
    , pr.dxc_salmonyearclass as salmonyearclass

    , pr.dxc_salmoninputid as salmoninputid
    , pr.dxc_salmonfishgroupnumber as salmonfishgroupnumber
    , pr.dxc_salmoncompanyid as salmoncompanyid
    , pr.dxc_salmonhatcherysupplierid as salmonhatcherysupplierid
    /*
    ,pr.workerresponsible
    ,pr.workerresponsiblefinancial
    ,pr.workerresponsiblesales
    */
    , pr.dxc_salmonsiteid as salmonsiteid

    , pr.dxc_salmonstageid as salmonstageid
    , pr.dxc_salmontypeid as salmontypeid
    , pr.dxc_musselfarm as musselfarm
    , pr.dxc_musselline as musselline
    , pr.dxc_musselcropid as musselcropid
    , pr.dxc_musselspattypeid as musselspattypeid
    , pr.dxc_musselcroptypeid as musselcroptypeid
    , pr.dxc_musselgrowingareaid as musselgrowingareaid
    , pr.dxc_musselsourcecropid1 as musselsourcecropid1
    , pr.dxc_musselmetreseeded1 as musselmetreseeded1
    , pr.dxc_musselsourcecropid2 as musselsourcecropid2
    , pr.dxc_musselmetreseeded2 as musselmetreseeded2
    , pr.partition
    , pr.[IsDelete]
    , case when cast(pr.created as date) = '1900-01-01' then null else cast(pr.created as date) end as created
    , concat(pr.projid, ' - ', pr.name) as proj_desc
    , case when cast(pr.startdate as date) = '1900-01-01' then null else cast(pr.startdate as date) end as startdate
    , case when cast(pr.projectedstartdate as date) = '1900-01-01' then null else cast(pr.projectedstartdate as date) end as projectedstartdate
    , case when cast(pr.projectedenddate as date) = '1900-01-01' then null else cast(pr.projectedenddate as date) end as projectedenddate
    , case when cast(pr.psaschedstartdate as date) = '1900-01-01' then null else cast(pr.psaschedstartdate as date) end as psaschedstartdate
    , case when cast(pr.psaschedenddate as date) = '1900-01-01' then null else cast(pr.psaschedenddate as date) end as psaschedenddate
    , cast(prcreateddatetime.newzealandtime as date) as createddatetime
    , cast(prmodifieddatetime.newzealandtime as date) as modifieddatetime
    , upper(pr.dataareaid) as project_dataareaid
    , pr.versionnumber
    , pr.sysrowversion
from {{ source('fno', 'projtable') }} as pr
cross apply dbo.f_get_enum_translation('projtable', '1033') as e
left join {{ source('fno', 'hcmworker') }} as w on cast(pr.workerresponsible as varchar) = cast(w.recid as varchar) and w.[IsDelete] is null
left join {{ ref('dim_d365_party') }} as pt on w.person = pt.party_recid and pt.[IsDelete] is null
left join {{ ref('dim_d365_projgroup') }} as pg on pr.projgroupid = pg.projgroupid and upper(pr.dataareaid) = pg.projgroup_dataareaid and pg.[IsDelete] is null
left join {{ ref('dim_d365_financialdimensionvalueset') }} as fd on pr.defaultdimension = fd.financialdimensionvalueset_recid
cross apply dbo.f_convert_utc_to_nzt(pr.createddatetime) as prcreateddatetime
cross apply dbo.f_convert_utc_to_nzt(pr.modifieddatetime) as prmodifieddatetime
{%- if is_incremental() %}
    where pr.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pr.[IsDelete] is null
{% endif %}
