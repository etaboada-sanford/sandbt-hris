{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_address_sk'],
    post_hook = 'delete from {{ this }} where [IsDelete] is not null;'
) }}

select
    a.[Id] as dim_d365_address_sk
    , a.recid as address_recid
    , a.address
    , a.city
    , a.cityrecid
    , a.county
    , a.district
    , a.districtname
    , a.isprivate as address_isprivate
    , a.latitude
    , a.longitude
    , a.state
    , a.street
    , a.validfrom
    , a.validto
    , a.zipcode
    , a.postbox
    , a.dxc_exportdeliveryterms
    , a.dxc_deliveryzone
    , a.dxc_locationcode
    , a.partition
    , a.[IsDelete]
    , a.versionnumber
    , a.sysrowversion
    , upper(a.countryregionid) as countryregionid
    , case
        when
            a.validto >= getdate()
            or a.validto is null then 1
        else 0
    end as isvalid
from {{ source('fno', 'logisticspostaladdress') }} as a
{%- if is_incremental() %}
    where a.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where a.[IsDelete] is null
{% endif %}
