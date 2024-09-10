{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_state_sk']
) }}

select
    las.[Id] as dim_d365_state_sk
    , las.recid as state_recid
    , las.name as state_name
    , las.stateid
    , las.partition
    , upper(las.countryregionid) as countryregionid
    , las.[IsDelete]
    , las.versionnumber
    , las.sysrowversion
from {{ source('fno', 'logisticsaddressstate') }} as las
{%- if is_incremental() %}
    where las.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where las.[IsDelete] is null
{% endif %}
