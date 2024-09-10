{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_job_sk']
) }}

select
    j.[Id] as dim_d365_job_sk
    , j.recid as job_recid
    , j.jobid
    , j.partition
    , j.recversion
    , j.[IsDelete]
    , j.versionnumber
    , j.sysrowversion
from {{ source('fno', 'hcmjob') }} as j
{%- if is_incremental() %}
    where j.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where j.[IsDelete] is null
{% endif %}