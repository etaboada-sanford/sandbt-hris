{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_worker_sk']
) }}

select
    w.[Id] as dim_d365_worker_sk
    , w.recid as worker_recid
    , w.person as party_recid
    , w.personnelnumber
    , w.partition
    , w.recversion
    , w.[IsDelete]
    , pt.name as worker_name
    , pt.namealias
    , pt.primary_email
    , pt.primary_ph
    , w.versionnumber
    , w.sysrowversion
from {{ source('fno', 'hcmworker') }} as w
inner join {{ ref('dim_d365_party') }} as pt
    on w.person = pt.party_recid
        and pt.[IsDelete] is null
{%- if is_incremental() %}
    where w.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where  w.[IsDelete] is null
{% endif %}