{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_itemtrackingdimensiongroup_sk']
) }}

select
    tdg.[Id] as dim_d365_itemtrackingdimensiongroup_sk
    , tdg.recid as itemtrackingdimensiongroup_recid
    , tdg.name as itemtrackingdimensiongroupid
    , tdg.description as itemtrackingdimensiongroup_name
    , tdg.captureserial
    , tdg.isserialatconsumptionenabled
    , tdg.isserialnumbercontrolenabled
    , tdg.[IsDelete]
    , tdg.versionnumber
    , tdg.sysrowversion
from {{ source('fno', 'ecorestrackingdimensiongroup') }} as tdg
{%- if is_incremental() %}
    where tdg.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where tdg.[IsDelete] is null
{% endif %}