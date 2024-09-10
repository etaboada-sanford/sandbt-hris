{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_pdsbatchattrib_enum_sk']
) }}

with uniquevalues as (
    select distinct mserp_dxc_pdsitembatchattributeenumerationvalueentityid
    from {{ source('mserp', 'dxc_pdsitembatchattributeenumerationvalueentity') }}
)

, convertedvalues as (
    select
        mserp_dxc_pdsitembatchattributeenumerationvalueentityid
        , row_number() over (order by mserp_dxc_pdsitembatchattributeenumerationvalueentityid) as rownum
    from uniquevalues
)

, recids as (
    select
        mserp_dxc_pdsitembatchattributeenumerationvalueentityid
        , cast('5637146076' + cast(rownum as varchar) as bigint) as recid
    from convertedvalues
)

select
    p.mserp_dxc_pdsitembatchattributeenumerationvalueentityid as dim_d365_pdsbatchattrib_enum_sk
    , r.recid
    , p.mserp_enumerationvalue as pdsattrib_enum_value
    , p.mserp_itembatchattributeid
    , p.mserp_dxc_pdsbatchattribenumdescription as pdsbatchattrib_enum_desc
    , p.mserp_dxc_configid as configid
    , p.mserp_dxc_speciescode as speciescode
    , upper(p.mserp_dataareaid) as pdsbatchattrib_enum_dataareaid
from {{ source('mserp', 'dxc_pdsitembatchattributeenumerationvalueentity') }} as p
inner join recids as r on p.mserp_dxc_pdsitembatchattributeenumerationvalueentityid = r.mserp_dxc_pdsitembatchattributeenumerationvalueentityid
