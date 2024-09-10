{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_postcode_sk']
) }}

select
    l.[Id] as dim_d365_postcode_sk
    , l.recid as postcode_recid
    , l.city
    , l.cityalias
    , l.cityrecid
    , l.district
    --county,
    , l.districtname
    , l.streetname
    , l.state
    , l.zipcode as postcode
    , l.partition
    , l.[IsDelete]
    , upper(l.countryregionid) as countryregionid
    , l.versionnumber
    , l.sysrowversion
from {{ source('fno', 'logisticsaddresszipcode') }} as l
{%- if is_incremental() %}
    where l.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where l.[IsDelete] is null
{% endif %}