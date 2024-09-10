{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_projectcategory_sk']
) }}

select
    pc.[Id] as dim_d365_projectcategory_sk
    , pc.categoryid
    , pc.name as category_name
    , pc.categorygroupid
    , pc.active
    , upper(pc.dataareaid) as projectcategory_dataareaid
    , pc.partition
    , pc.[IsDelete]
    , pc.versionnumber
    , pc.sysrowversion
from {{ source('fno', 'projcategory') }} as pc
{%- if is_incremental() %}
    where pc.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where pc.[IsDelete] is null
{% endif %}