{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_function_sk']
) }}

with ct as (
    select distinct
        d4.recid as functionid
        , d4.value as functionvalue
        , d4.description as function_name
        , d4.partition
        , d4.[IsDelete]
        , d4.versionnumber
        , d4.sysrowversion
    from {{ source('fno', 'dimensionattributevaluecombination') }} as d
    inner join {{ source('fno', 'dimensionfinancialtag') }} as d4 on d.d4_function = d4.recid
    {%- if is_incremental() %}
        where d4.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where  d4.[IsDelete] is null
    {% endif %}
)

select
    {{ dbt_utils.generate_surrogate_key(['ct.functionid']) }} as dim_d365_function_sk
    , ct.*
from ct
