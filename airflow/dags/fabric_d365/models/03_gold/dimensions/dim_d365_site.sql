{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_site_sk']
) }}

with navision as (
    select
        {{ dbt_utils.generate_surrogate_key(['\'999999\'', '\'NAV999\'']) }} as dim_d365_site_sk
        , 999999 as site_recid
        , 'NAV999' as siteid
        , 'Navision not migrated' as site_name
        , 'SANF' as site_dataareaid
        , null as timezone
        , null as defaultdimension
        , null as defaultinventstatusid
        , null as dxc_defaultdimension
        , 0 as [IsDelete]
        , 0 as versionnumber
        , 0 as sysrowversion
)

select
    st.[Id] as dim_d365_site_sk
    , st.recid as site_recid
    , st.siteid
    , st.name as site_name
    , upper(st.dataareaid) as site_dataareaid
    , st.timezone
    , st.defaultdimension
    , st.defaultinventstatusid
    , st.dxc_defaultdimension
    , st.[IsDelete]
    , st.versionnumber
    , st.sysrowversion
from {{ source('fno', 'inventsite') }} as st
{%- if is_incremental() %}
    where st.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where st.[IsDelete] is null
{% endif %}
union all
/* Site not migrated from Navision */
select *
from navision
