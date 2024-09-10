{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_vendorinvoice_sk']
) }}

select
    j.[Id] as dim_d365_vendorinvoice_sk

    , j.recid as vendorinvoice_recid
    , j.internalinvoiceid
    , j.invoiceid
    , j.orderaccount

    , j.invoiceaccount
    , j.currencycode

    , j.invoiceamount
    , j.invoiceamountmst
    , j.invoiceroundoff
    , j.defaultdimension
    , j.deliveryname

    , j.deliverypostaladdress
    , j.remittanceaddress
    , j.description
    , j.dlvmode
    , j.dlvterm

    , j.documentnum
    , j.exchrate
    , j.reportingcurrencyexchangerate
    , j.intercompanysalesid
    , j.payment
    , j.postingprofile
    , j.purchid
    , j.purchasetype as purchasetypeid

    , {{ translate_enum('ept', 'j.purchasetype' ) }} as purchasetype
    , j.qty
    , j.salesbalance
    , upper(j.dataareaid) as vendorinvoice_dataareaid

    , cast(j.invoicedate as date) as invoicedate
    , cast(j.deliverydate_es as date) as deliverydate_es
    , cast(j.documentdate as date) as documentdate
    , cast(j.duedate as date) as duedate
    , cast(j.fixedduedate as date) as fixedduedate
    , case when cast(j.receiveddate as date) = '1900-01-01' then null else cast(j.receiveddate as date) end as receiveddate
    , upper(j.intercompanycompanyid) as intercompanycompanyid
    , j.versionnumber
    , j.sysrowversion
from {{ source('fno', 'vendinvoicejour') }} as j
cross apply dbo.f_get_enum_translation('vendinvoicejour', '1033') as ept
{%- if is_incremental() %}
    where j.sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where  j.[IsDelete] is null
{% endif %}
