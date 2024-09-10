{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_itemcoveragegroup_sk']
) }}

select
    r.[Id] as dim_d365_itemcoveragegroup_sk
    , r.recid as itemcoveragegroup_recid
    , r.reqgroupid as itemcoveragegroupid
    , r.name as itemcoveragegroup_name
    , r.actioncalc
    , r.actiontimefence
    , r.capacitytimefence
    , r.covperiod
    , r.covrule
    , r.covtimefence
    , r.explosiontimefence
    , r.futurescalc
    , r.masterplantimefence
    , r.maxnegativedays
    , r.maxpositivedays
    , upper(r.dataareaid) as itemcoveragegroup_dataareaid
    , r.partition
    , r.[IsDelete]
    , r.versionnumber
    , r.sysrowversion
from {{ source('fno', 'reqgroup') }} as r
{%- if is_incremental() %}
    where r.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where r.[IsDelete] is null
{% endif %}