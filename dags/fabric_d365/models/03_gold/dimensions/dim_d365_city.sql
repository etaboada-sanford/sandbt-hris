{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_city_sk']
) }}

select
    la.[Id] as dim_d365_city_sk
    , la.recid as city_recid
    , la.citykey
    , la.description as city_desc
    , la.name as city_name
    , la.stateid
    , la.partition
    , la.[IsDelete]
    , la.versionnumber
    , la.sysrowversion
    , upper(la.countryregionid) as countryregionid
from
    {{ source('fno', 'logisticsaddresscity') }} as la
{%- if is_incremental() %}
    where la.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where la.[IsDelete] is null
{% endif %}
