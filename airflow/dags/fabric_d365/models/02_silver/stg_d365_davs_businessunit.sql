{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_businessunit_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as stg_d365_davs_businessunit_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d1_businessunit
    , d.d1_businessunitvalue
    , d1pt.name as d1_businessunit_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join
    {{ source('fno', 'omoperatingunit') }} as d1
    on d.d1_businessunit = d1.recid
left join
    {{ source('fno', 'dirpartytable') }} as d1pt
    on d1.recid = d1pt.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
