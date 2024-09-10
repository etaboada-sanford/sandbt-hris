{{ config(
    materialized = 'incremental', 
    unique_key = ['stg_d365_davs_department_sk'],
    tags=["dimensionattributevalueset"]
) }}

select
    d.[Id] as stg_d365_davs_department_sk
    , d.recid as financialdimensionvalueset_recid
    , d.d3_department
    , d.d3_departmentvalue
    , d3.description as d3_department_name
    , d.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
from
    {{ source('fno', 'dimensionattributevalueset') }} as d
left join {{ source('fno', 'dimensionfinancialtag') }} as d3 on d.d3_department = d3.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
