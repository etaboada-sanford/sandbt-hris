{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_vendor_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as stg_d365_davs_vendor_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d6_vendor
    , d.d6_vendorvalue
    , d6pt.name as d6_vendor_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'vendtable') }} as d6 on d.d6_vendor = d6.recid
left join {{ source('fno', 'dirpartytable') }} as d6pt on d6.party = d6pt.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
