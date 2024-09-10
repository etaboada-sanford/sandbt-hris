{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_vendorpackingslip_sk']
) }}

select
    v.[Id] as dim_d365_vendorpackingslip_sk

    , v.recid as vendorpackingslip_recid
    , v.orderaccount

    , v.invoiceaccount
    , v.packingslipid
    , v.defaultdimension

    , v.deliveryname
    , v.deliverypostaladdress
    , v.dlvmode
    , v.dlvterm

    , v.intercompanysalesid
    , v.purchid
    , v.purchasetype as purchasetypeid

    , {{ translate_enum('ept', 'v.purchasetype' ) }} as purchasetype
    , v.[IsDelete]
    , v.versionnumber

    , v.sysrowversion
    , upper(v.dataareaid) as vendorpackingslip_dataareaid
    , cast(v.deliverydate as date) as deliverydate

    , case when cast(v.documentdate as date) = '1900-01-01' then null else cast(v.documentdate as date) end as documentdate
    , upper(v.intercompanycompanyid) as intercompanycompanyid
    , coalesce(p.name, po.orderer) as requester
from {{ source('fno', 'vendpackingslipjour') }} as v
cross apply dbo.f_get_enum_translation('vendpackingslipjour', '1033') as ept
left join {{ source('fno', 'hcmworker') }} as w on v.requester = w.recid
left join {{ ref('dim_d365_party') }} as p on w.person = p.party_recid
left join {{ ref('dim_d365_purchaseorder') }} as po on upper(v.dataareaid) = po.purchaseorder_dataareaid and v.purchid = po.purchid
{%- if is_incremental() %}
    where v.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where  v.[IsDelete] is null
{% endif %}
