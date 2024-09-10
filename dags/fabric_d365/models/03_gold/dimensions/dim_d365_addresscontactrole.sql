{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_addresscontactrole_sk']
) }}

select
    ll.[Id] as dim_d365_addresscontactrole_sk
    , ll.recid as addresscontactrole_recid
    , ll.iscontactinfo
    , ll.ispostaladdress
    , ll.name as addresscontactrole_name
    , ll.type as addresscontactrole_type
    , ll.partition
    , ll.[IsDelete]
    , ll.versionnumber
    , ll.sysrowversion
from {{ source('fno', 'logisticslocationrole') }} as ll
{%- if is_incremental() %}
    where ll.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where ll.[IsDelete] is null
{% endif %}
