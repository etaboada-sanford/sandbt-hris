{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_triptype_sk']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['dt.mserp_triptypeid']) }}  as dim_d365_triptype_sk
    , dt.mserp_triptypeid as triptype_recid
    , dt.mserp_triptypeid as triptypeid
    , dt.mserp_description as description
    , dt.mserp_autocompletepricing as autocompletepricing
    , dt.mserp_autocompletequota as autocompletequota
    , dt.mserp_allowpurchreceiptingbeforequotacomplete as allowpurchreceiptingbeforequotacomplete
    , dt.mserp_autocompleteorder as autocompleteorder
    , dt.mserp_autopostreceipt as autopostreceipt
    , dt.mserp_useforecastquantity as useforecastquantity
    , dt.mserp_farmtype as farmtype
    , upper(dt.mserp_dataareaid) as dxc_triptype_dataareaid
    , null as [IsDelete]
    , 0 as versionnumber
    , 0 as sysrowversion
from {{ source('mserp', 'dxc_triptype') }} as dt
where dt.[IsDelete] is null