{{ config(
    materialized = 'view',
    tags=["logisticselectronicaddress"]
) }}

select *
from {{ ref('stg_d365_lea_contact') }}
where upper(role) like '%PO CONFIRM%'
