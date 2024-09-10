{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_customer_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as STG_d365_davs_customer_sk
    , d.recid as financialdimensionvalueset_recid
                           , d.d5_customer
                 , d.d5_customervalue
    , d5pt.name as d5_customer_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left JOIN {{ source('fno', 'custtable') }} as d5 on d.d5_customer = d5.recid
left join {{ source('fno', 'dirpartytable') }} as d5pt on d5.party = d5pt.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
