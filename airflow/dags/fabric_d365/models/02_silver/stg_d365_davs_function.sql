{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_function_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as stg_d365_davs_function_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d4_function
    , d.d4_functionvalue
    , d4.description as d4_function_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'dimensionfinancialtag') }} as d4 on d.d4_function = d4.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
