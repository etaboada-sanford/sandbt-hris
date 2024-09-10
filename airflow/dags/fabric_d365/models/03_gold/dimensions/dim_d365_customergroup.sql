{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_customergroup_sk']
) }}

select
    cg.[Id] as dim_d365_customergroup_sk
    , cg.recid as customergroup_recid
    , cg.custgroup as customergroup
    , cg.defaultdimension
    , cg.name as customergroup_name
    , cg.paymtermid
    , cg.partition
    , cg.[IsDelete]
    , cg.versionnumber
    , cg.sysrowversion
    , upper(cg.dataareaid) as customergroup_dataareaid
from {{ source('fno', 'custgroup') }} as cg
{%- if is_incremental() %}
    where cg.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where cg.[IsDelete] is null
{% endif %}
