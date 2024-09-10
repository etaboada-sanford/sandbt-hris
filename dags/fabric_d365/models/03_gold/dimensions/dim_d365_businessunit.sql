{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_businessunit_sk']
) }}

select distinct
    d.[Id] as dim_d365_businessunit_sk
    , d.d1_businessunit as businessunitid
    , d.d1_businessunitvalue as businessunitvalue
    , d1pt.name as businessunit_name
    , d1.omoperatingunittype
    , d1.[IsDelete]
    , d.versionnumber
    , d.sysrowversion
    , concat(upper(d.d1_businessunitvalue), ' - ', coalesce(d1pt.name, '')) as businessunit
from {{ source('fno', 'dimensionattributevaluecombination') }} as d
inner join {{ source('fno', 'omoperatingunit') }} as d1
    on d.d1_businessunit = d1.recid
inner join {{ source('fno', 'dirpartytable') }} as d1pt
    on d1.recid = d1pt.recid
{%- if is_incremental() %}
    where d.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where d.[IsDelete] is null
{% endif %}
