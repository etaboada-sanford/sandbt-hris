{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_country_sk']
) }}

select
    cr.[Id] as dim_d365_country_sk
    , cr.recid as country_recid
    , cr.isocode as country_isocode
    , crt.shortname as country_name
    , crt.longname as country_lname
    , cr.partition
    , cr.[IsDelete]
    , cr.versionnumber
    , cr.sysrowversion
    , upper(cr.countryregionid) as countryregionid
    , replace(mc.tm1_continent, ' Sales Destination', '') as continent
from {{ source('fno', 'logisticsaddresscountryregion') }} as cr
inner join {{ source('fno', 'logisticsaddresscountryregiontranslation') }} as crt
    on upper(cr.countryregionid) = upper(crt.countryregionid)
        and crt.languageid = 'en-NZ'
        and crt.[IsDelete] is null
left join {{ ref('stg_map_d365_nav_country') }} as mc on upper(crt.countryregionid) = upper(mc.d365_country_code)
{% if is_incremental() %}
    where cr.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where cr.[IsDelete] is null
{% endif %}
