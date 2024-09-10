{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_costcenter_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as stg_d365_davs_costcenter_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d2_costcenter
    , d.d2_costcentervalue
    , d2pt.name as d2_costcenter_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'omoperatingunit') }} as d2 on d.d2_costcenter = d2.recid
left join {{ source('fno', 'dirpartytable') }} as d2pt on d2.recid = d2pt.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
