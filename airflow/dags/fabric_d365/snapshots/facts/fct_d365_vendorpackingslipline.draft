
 select
    pl.[Id] as fact_d365_vendorpackingslipline_sk
    , pl._sysrowid vendorpackingslipline_recid 
    , upper(pl.dataareaid) vendorpackingslipline_dataareaid

    , pl.currencycode_w currencycode
    , pl.externalitemid
    , pl.itemid
    , pl.stockedproduct
    , pl.linenum
    , pl.lineamount_w lineamount
    , pl.name
    , pl.qty
    , pl.inventqty
    , pl.ordered    /* do not sum */
    , pl.remain     /* do not sum */
    , pl.remaininvent     /* do not sum */
    , pl.valuemst
    , pl.purchunit
    , pl.cancelledqty
    , pl.dlvmode

    , pl.purchaselinelinenumber
    , pol.approver

    , pl.fullymatched
    , pl.intercompanyinventtransid
    , pl.inventrefid
    , pl.inventreftransid
    , pl.inventreftype inventreftypeid

    , eirt.dim_d365_enum_sk dim_d365_inventreftranstype_enum_sk
  
    , ps.dim_d365_vendorpackingslip_sk
    , po.dim_d365_purchaseorder_sk
    , i.dim_d365_item_sk
    , vi.dim_d365_vendor_sk dim_d365_vendorinvoice_sk
    , vo.dim_d365_vendor_sk dim_d365_vendororder_sk
    
    , cat.dim_d365_category_sk
    
    , id.dim_d365_inventdim_sk
    , s.dim_d365_site_sk
    , w.dim_d365_warehouse_sk

    , fd.dim_d365_financialdimensionvalueset_sk
    , bu.dim_d365_businessunit_sk
    , cc.dim_d365_costcenter_sk
    , dep.dim_d365_department_sk
    , fun.dim_d365_function_sk
    
    , dtdel.dim_date_key dim_date_delivery_key
    , dtacc.dim_date_key dim_date_accounting_key
    , dtinv.dim_date_key dim_date_invent_key
    
from {{source('fno', 'vendpackingsliptrans')}} pl

join {{ref('dim_d365_vendorpackingslip')}} ps on pl.vendpackingslipjour = ps.vendorpackingslip_recid

left join {{ref('dim_d365_purchaseorder')}} po on pl.origpurchid = po.purchid

left join {{ref('fct_d365_purchaseorderline')}} pol on po.dim_d365_purchaseorder_sk = pol.dim_d365_purchaseorder_sk
    and pl.purchaselinelinenumber = pol.linenumber

left join {{ref('dim_d365_item')}} i on pl.itemid = i.itemid
    and upper(pl.dataareaid) = upper(i.item_dataareaid)

left join {{ref('dim_d365_vendor')}} vi on ps.invoiceaccount = vi.accountnum
    and upper(ps.vendorpackingslip_dataareaid) = vi.vendor_dataareaid

left join {{ref('dim_d365_vendor')}} vo on ps.orderaccount = vo.accountnum
    and upper(ps.vendorpackingslip_dataareaid) = vo.vendor_dataareaid

left join {{ref('dim_d365_category')}} cat on pl.procurementcategory = cat.category_recid

left join {{ref('dim_d365_inventdim')}} id on pl.inventdimid = id.inventdimid
    and upper(pl.dataareaid) = upper(id.inventdim_dataareaid)
    and pl.inventdimid != 'AllBlank'

left join {{ref('dim_d365_site')}} s on id.inventsiteid = s.siteid
    and id.inventdim_dataareaid = s.site_dataareaid
    
left join {{ref('dim_d365_warehouse')}} w on id.inventlocationid = w.warehouseid
    and id.inventsiteid = w.siteid
    and id.inventdim_dataareaid = w.warehouse_dataareaid

left join {{ref('dim_d365_financialdimensionvalueset')}} fd on pl.defaultdimension = fd.financialdimensionvalueset_recid
left join {{ref('dim_d365_businessunit')}} bu on fd.d1_businessunit = bu.businessunitid
left join {{ref('dim_d365_costcenter')}} cc on fd.d2_costcenter = cc.costcenterid
left join {{ref('dim_d365_department')}} dep on fd.d3_department = dep.departmentid
left join {{ref('dim_d365_function')}} fun on fd.d4_function = fun.functionid

left join {{ref('dim_date')}} dtdel on to_date(pl.deliverydate) = to_date(dtdel.calendar_date)
left join {{ref('dim_date')}} dtacc on to_date(pl.accountingdate) = to_date(dtacc.calendar_date)
left join {{ref('dim_date')}} dtinv on to_date(pl.inventdate) = to_date(dtinv.calendar_date)

left join {{ref('dim_d365_enum')}} eirt on eirt.enumname = 'InventTransType'
    and eirt.enum_item_value = pl.inventreftype

where pl.is_deleted = 0