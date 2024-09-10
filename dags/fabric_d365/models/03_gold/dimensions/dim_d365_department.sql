{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_department_sk']
) }}

select distinct
    d3.[Id] as dim_d365_department_sk
    , d3.recid as departmentid
    , d3.value as departmentvalue
    , d3.description as department_name
    , d3.partition
    , d3.[IsDelete]
    , d3.versionnumber
    , d3.sysrowversion
    , concat(upper(d3.value), ' - ', coalesce(d3.description, '')) as department
from {{ source('fno', 'dimensionattributevaluecombination') }} as d
inner join {{ source('fno', 'dimensionfinancialtag') }} as d3 on d.d3_department = d3.recid
{%- if is_incremental() %}
    where d3.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null and d3.[IsDelete] is null
{% endif %}
