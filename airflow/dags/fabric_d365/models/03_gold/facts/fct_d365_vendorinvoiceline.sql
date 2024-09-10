{{ config(
    materialized = 'incremental', 
    unique_key = ['fct_d365_vendorinvoiceline_sk']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['l.recid']) }} as fct_d365_vendorinvoiceline_sk
    , l.recid as invoiceline_recid
    , j.recid as invoicejour_recid

    , po.dim_d365_purchaseorder_sk
    , poo.dim_d365_purchaseorder_sk as dim_d365_orig_purchaseorder_sk
    , vi.dim_d365_vendor_sk as dim_d365_vendor_invoice_sk
    , vo.dim_d365_vendor_sk as dim_d365_vendor_order_sk
    , i.dim_d365_item_sk
    , co.dim_d365_company_sk
    , cur.dim_d365_currency_sk
    , id.dim_d365_inventdim_sk
    , fd.dim_d365_financialdimensionvalueset_sk
    , fa.dim_d365_financialdimensionaccount_sk
    , bu.dim_d365_businessunit_sk
    , cc.dim_d365_costcenter_sk
    , dep.dim_d365_department_sk
    , fun.dim_d365_function_sk
    , cat.dim_d365_category_sk
    , dt.dim_date_key as dim_invoicedate_key

    , l.invoiceid
    , l.internalinvoiceid
    , l.invoicedate
    , j.documentdate
    , l.purchid
    , l.origpurchid
    , j.orderaccount
    , j.invoiceaccount
    , j.duedate

    , j.costledgervoucher
    , j.ledgervoucher
    , j.description as invoice_description

    , l.currencycode

    , l.inventdate
    , l.inventdimid
    , l.inventqty
    , l.inventrefid
    , l.inventtransid
    , l.inventreftype
    , l.inventreftransid
    , l.itemid

    , l.linenum
    , l.name as line_description
    , l.qty

    , l.lineamount as lineamount_lcy
    , l.lineamountmst
    , round(case when l.currencycode = 'NZD' then l.lineamount
        when j.exchrate != 100 then l.lineamount / (1 / (j.exchrate) * 100)
        when j.reportingcurrencyexchangerate != 100 then l.lineamount / (1 / (j.reportingcurrencyexchangerate) * 100)
        else l.lineamount / ex.exchangerate
    end, 2) as lineamount_nzd

    , l.purchaselinelinenumber
    , l.purchprice as purchprice_lcy
    , round(case when l.currencycode = 'NZD' then l.purchprice
        when j.exchrate != 100 then l.purchprice / (1 / (j.exchrate) * 100)
        when j.reportingcurrencyexchangerate != 100 then l.purchprice / (1 / (j.reportingcurrencyexchangerate) * 100)
        else l.purchprice / ex.exchangerate
    end, 2) as purchprice_nzd

    , upper(l.purchunit) as purchunit
    , upper(l.dataareaid) as vendorinvoiceline_dataareaid
    , l.versionnumber
    , l.sysrowversion

from {{ source('fno', 'vendinvoicetrans') }} as l
inner join {{ source('fno', 'vendinvoicejour') }} as j
    on
        l.invoiceid = j.invoiceid
        and l.internalinvoiceid = j.internalinvoiceid

left join
    {{ ref('fct_d365_defaultexchangerate') }}
        as ex
    on ex.fromcurrencycode = 'NZD'
        and l.currencycode = ex.tocurrencycode
        and l.invoicedate >= ex.validfrom and l.invoicedate <= ex.validto

left join {{ ref('dim_d365_purchaseorder') }} as po on l.purchid = po.purchid
left join {{ ref('dim_d365_purchaseorder') }} as poo on l.origpurchid = poo.purchid
left join {{ ref('dim_d365_vendor') }} as vi on j.invoiceaccount = vi.accountnum and upper(j.dataareaid) = upper(vi.vendor_dataareaid)
left join {{ ref('dim_d365_vendor') }} as vo on j.orderaccount = vo.accountnum and upper(j.dataareaid) = upper(vo.vendor_dataareaid)
left join {{ ref('dim_d365_item') }} as i on l.itemid = i.itemid and upper(l.dataareaid) = upper(i.item_dataareaid)
left join {{ ref('dim_d365_currency') }} as cur on l.currencycode = cur.currencycode
left join {{ ref('dim_d365_company') }} as co on upper(l.dataareaid) = co.company_dataarea
left join {{ ref('dim_d365_financialdimensionvalueset') }} as fd on l.defaultdimension = fd.financialdimensionvalueset_recid
left join {{ ref('dim_d365_financialdimensionaccount') }} as fa on l.ledgerdimension = fa.financialdimensionaccount_recid

left join {{ ref('dim_d365_businessunit') }} as bu on fd.d1_businessunit = bu.businessunitid
left join {{ ref('dim_d365_costcenter') }} as cc on fd.d2_costcenter = cc.costcenterid
left join {{ ref('dim_d365_department') }} as dep on fd.d3_department = dep.departmentid
left join {{ ref('dim_d365_function') }} as fun on fd.d4_function = fun.functionid

left join
    {{ ref('dim_d365_inventdim') }}
        as id
    on l.inventdimid = id.inventdimid
        and id.inventdimid != 'AllBlank'
left join {{ ref('dim_d365_category') }} as cat on l.procurementcategory = cat.category_recid
left join {{ ref('dim_date') }} as dt on convert(date, l.invoicedate) = convert(date, dt.calendar_date)
