{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_charge_sk']
) }}

select
    m.[Id] as dim_d365_charge_sk
    , m.recid as charge_recid
    , m.markupcode as chargecode
    , m.maxamount
    , m.moduletype
    , m.taxitemgroup
    , m.txt as charge_desc
    , m.customerledgerdimension
    , m.custtype
    , m.custposting
    , {{ translate_enum('markuptable_enum', 'm.custposting' ) }} as customer_posting
    , m.vendorledgerdimension

    , m.vendtype
    , m.vendposting
    , {{ translate_enum('markuptable_enum', 'm.vendposting' ) }} as vendor_posting
    , m.partition
    , m.[IsDelete]
    , m.versionnumber
    , m.sysrowversion
    , case
        when m.moduletype = 1 then 'Account Receivable'
        when m.moduletype = 2 then 'Account Payable'
        else ''
    end as chargetype
    , upper(m.dataareaid) as charge_dataareaid
from {{ source('fno', 'markuptable') }} as m
cross apply dbo.f_get_enum_translation('markuptable', '1033') as markuptable_enum
{%- if is_incremental() %}
    where m.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where m.[IsDelete] is null
{% endif %}
