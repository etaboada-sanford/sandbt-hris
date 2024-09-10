{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_mainaccount_sk']
) }}

select
    m.[Id] as dim_d365_mainaccount_sk
    , m.recid as mainaccount_recid
    , m.mainaccountid
    , m.name as mainaccount_name
    , concat(m.mainaccountid, ' - ', m.name) as mainaccount
    , m.dxc_workflowspendtypeid
    , m.accountcategoryref
    , m.partition
    , m.[IsDelete]
    , l.gl_calc_factor
    , l.gl_category_l0
    , l.gl_category_l1
    , l.gl_category_l2
    , l.gl_category_l3
    , l.gl_category_l4
    , l.gl_category_l5
    , l.gl_category_l6
    , l.gl_category_l7
    , l.gl_category_l8
    , l.gl_category_l9
    , l.gl_category_l10
    , l.gl_category_l11
    , l.gl_category_l12
    , l.gl_category_l13
    , m.versionnumber
    , m.sysrowversion
from {{ source('fno', 'mainaccount') }} as m
left join {{ ref('stg_dim_tm1_gl_category_level') }} as l on m.mainaccountid = l.mainaccountid
{%- if is_incremental() %}
    where m.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where m.[IsDelete] is null
{% endif %}