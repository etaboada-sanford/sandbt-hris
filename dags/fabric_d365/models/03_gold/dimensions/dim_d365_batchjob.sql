{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_batchjob_sk']
) }}

select
    bj.[Id] as dim_d365_batchjob_sk
    , bj.recid as batchjob_recid
    , bj.caption
    , bj.startdatetime
    , bj.enddatetime
    , bj.executingby
    , bj.[IsDelete]
    , bj.versionnumber
    , bj.sysrowversion
    , upper(
        bj.company
    ) as batchjob_dataareaid
from {{ source('fno', 'batchjob') }} as bj
{%- if is_incremental() %}
    where bj.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where bj.[IsDelete] is null
{% endif %}
