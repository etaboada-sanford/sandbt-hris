{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_costgroup_sk']
) }}

select
    cg.[Id] as dim_d365_costgroup_sk
    , cg.recid as costgroup_recid
    , cg.costgroupid
    , cg.name as costgroup_name
    , cg.dxc_bomfinishedgooditem
    , cg.dxc_bominputitem
    , {{ translate_enum('bomcostgroup_enum', 'cg.costgrouptype' ) }} as costgrouptype
    , cg.[IsDelete]
    , cg.versionnumber
    , cg.sysrowversion
    , upper(cg.dataareaid) as costgroup_dataareaid
from {{ source('fno', 'bomcostgroup') }} as cg
cross apply dbo.f_get_enum_translation('bomcostgroup', '1033') as bomcostgroup_enum
{%- if is_incremental() %}
    where cg.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where cg.[IsDelete] is null
{% endif %}
