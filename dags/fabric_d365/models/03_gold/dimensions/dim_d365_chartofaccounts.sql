{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_chartofaccounts_sk']
) }}

select
    ac.[Id] as dim_d365_chartofaccounts_sk
    , ac.recid as chartofaccount_recid
    , ac.mainaccountid as chartofaccount
    , ac.name as chartofaccount_name
    , ac.accountcategoryref
    , acc.accountcategory
    , acc.accounttype
    , ac.[IsDelete]
    , ac.versionnumber
    , ac.sysrowversion
from {{ source('fno', 'mainaccount') }} as ac
left join {{ source('fno', 'mainaccountcategory') }} as acc
    on ac.accountcategoryref = acc.accountcategoryref
{%- if is_incremental() %}
    where ac.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where ac.[IsDelete] is null
{% endif %}
