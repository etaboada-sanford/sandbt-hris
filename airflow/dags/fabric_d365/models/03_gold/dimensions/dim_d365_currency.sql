{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_currency_sk']
) }}

select
    c.[Id] as dim_d365_currency_sk
    , c.recid as currency_recid
    , c.currencycode
    , c.symbol
    , c.roundingprecision
    , c.txt as currency_name
    , c.partition
    , c.[IsDelete]
    , c.versionnumber
    , c.sysrowversion
from {{ source('fno', 'currency') }} as c
{%- if is_incremental() %}
    where c.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where c.[IsDelete] is null
{% endif %}
