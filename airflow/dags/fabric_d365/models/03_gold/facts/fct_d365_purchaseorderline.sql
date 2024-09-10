{{ config(
    materialized = 'incremental', 
    unique_key = ['exchangerate_recid']
) }}

with 
/* purchase order lines to 'receipt' matched quantity
 distinct needed as vendinvoicepackingslipquantitymatch recids may not be unique in stg_fct_d365_vendorinvoicematching_1
 */
ctepackingslipmatch as (
    select pl.recid purchaseorderline_recid
        , pl.versionnumber
        , pl.sysrowversion
        , round(sum(vipsqm.quantity), 3) qty
    from {{source('fno', 'purchline')}} pl
    join (
        select distinct
            vim.pl_recid
            , vim.vipsqm_recid
        from {{ref('stg_fct_d365_vendorinvoicematching_1')}} vim
        ) vim on pl.recid = vim.pl_recid
    join {{source('fno', 'vendinvoicepackingslipquantitymatch')}} vipsqm on vipsqm.recid = vim.vipsqm_recid
    group by pl.recid, pl.versionnumber, pl.sysrowversion
    )
,
/* Receipts against the line - quantity
 distinct needed as vendpackingsliptrans recids may not be unique in stg_fct_d365_vendorinvoicematching_1
*/
ctepackingslip as (
    select pl.recid purchaseorderline_recid
        , pl.versionnumber
        , pl.sysrowversion
        , round(sum(vpsl.qty), 3) qty
    from {{source('fno', 'purchline')}} pl
    join (
        select distinct
            vim.pl_recid
            , vim.vpsl_recid
        from {{ref('stg_fct_d365_vendorinvoicematching_1')}} vim
        ) vim on pl.recid = vim.pl_recid
    join {{source('fno', 'vendpackingsliptrans')}} vpsl on vim.vpsl_recid = vpsl.recid
    group by pl.recid, pl.versionnumber, pl.sysrowversion
    )
,

/* the max invoice date from ods_d365_vendinvoicepurchlink based on origpurchid
 can use the actual invoice line from stg_fct_d365_vendorinvoicematching_1
 distinct needed as vendinvoicetrans recids may not be unique in stg_fct_d365_vendorinvoicematching_1
 */
cteinv as (
    select pl.recid purchaseorderline_recid
        , vit.currencycode
        , pl.versionnumber
        , pl.sysrowversion
        , max(vit.invoicedate) as invoicedate
        , sum(vit.qty) as invoiceqty
        , sum(vit.lineamount) as invoicelineamount
    from {{source('fno', 'purchline')}} pl
    join (
        select distinct
            vim.pl_recid
            , vim.vit_recid
        from {{ref('stg_fct_d365_vendorinvoicematching_1')}} vim
        ) vim on pl.recid = vim.pl_recid
    join {{source('fno', 'vendinvoicetrans')}} vit on vit.recid = vim.vit_recid
    group by pl.recid, vit.currencycode, pl.versionnumber, pl.sysrowversion
)
,

vendorpct as (
    select *,
            /* For Mussel Harvest PO's take Vendor % into account  ticket #56146 */
            case 
                when t.vendaccount in ('SF499200', 'SF499201') then coalesce(1 - t.vendor1percent, 1) 
                when t.vendaccount in ('SF499210') then 1
                else coalesce(t.vendor1percent, 1)
            end as vendor1pct_
        from {{ref('dim_d365_triporder')}} t
),

tripord as (
    select *,
         case when coalesce(vendor1pct_, 0) = 0 then 1 else vendor1pct_ end as vendor1pct
    from vendorpct
),

ctpurchline as (
    select 
    {{ dbt_utils.generate_surrogate_key(['pl.recid'])}} as fact_purchaseorderline_sk,
    pl.recid  purchaseorderline_recid,
    pr.dim_d365_purchaserequisition_sk,

    v.dim_d365_vendor_sk,
    vi.dim_d365_vendor_sk dim_d365_vendor_invoice_sk,
    vo.dim_d365_vendor_sk dim_d365_vendor_order_sk,
    cat.dim_d365_category_sk,
    i.dim_d365_item_sk,
    w.dim_d365_warehouse_sk,
    s.dim_d365_site_sk,
    cur.dim_d365_currency_sk,
    co.dim_d365_company_sk,
    ctecomp.dim_d365_company_parent_sk dim_d365_company_parent_sk,
    po.dim_d365_purchaseorder_sk,
    t.dim_d365_triporder_sk,
    ig.dim_d365_itemgroup_sk,
    icg.dim_d365_itemcoveragegroup_sk,
    ibg.dim_d365_itembuyergroup_sk,
    istoragedim.dim_d365_itemstoragedimensiongroup_sk,
    ept.dim_d365_enum_sk dim_d365_enum_purchtype_sk,
    epts.dim_d365_enum_sk dim_d365_enum_purchstatus_sk,
    bc.dim_d365_enum_sk dim_d365_enum_barcodetype_sk,
    fd.dim_d365_financialdimensionvalueset_sk,
    fa.dim_d365_financialdimensionaccount_sk,
    /* mainaccount from ItemPosting by Item InventAccountType 3 = Purchase expenditure for product | PurchConsump 
        Posting by Category - Non-Item 63 = Purchase expenditure for expense | PurchExpense
    */
    coalesce(ip_all.dim_d365_mainaccount_sk, ip_cat.dim_d365_mainaccount_sk) dim_d365_mainaccount_sk,
    bu.dim_d365_businessunit_sk,
    cc.dim_d365_costcenter_sk,
    dep.dim_d365_department_sk,
    fun.dim_d365_function_sk,

    prj.dim_d365_project_sk,
    prc.dim_d365_projectcategory_sk,

    dt.dim_date_key dim_deliverydate_key,
    del_a.dim_d365_address_sk dim_d365_delivery_address_sk,
    del_country.dim_d365_country_sk  dim_d365_delivery_country_sk,
    id.dim_d365_inventdim_sk,
    ib.dim_d365_inventbatch_sk,
    fs.dim_d365_fishstate_sk,
    dti.dim_date_key dim_invoicedate_key,
    dtl.dim_date_key dim_landingdate_key,

    pl.recid recid, 
    upper(pl.dataareaid) dataareaid,
    pl.purchid,
    wf.approver,
    pl.barcode,
    pl.inventrefid,
    pl.inventreftransid,
    pl.inventtransid,
    pl.itemid,
    pl.name orderline_name,
    pl.lineamount,
    pl.linenumber,
    pl.purchmarkup,
    /* when purchprice is 0 use lineamount/purchqty */
    case when pl.purchprice = 0 and pl.purchqty != 0 then pl.lineamount/pl.purchqty else pl.purchprice end purchprice,
    pl.purchqty productweight_in_purchunit,
    /* use UoM to get weight in kG */
    round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) as purchline_productweight_kg,
    /* case when lower(pl.purchunit) = 'lb' then pl.purchqty*0.45359237 else pl.purchqty end purchline_productweight_kg, */
    pl.purchstatus,
    upper(pl.purchunit) purchunit,
    pl.qtyordered,
    pl.remainder,
    pl.remaininventfinancial,
    pl.remaininventphysical,
    pl.remainpurchfinancial,
    pl.remainpurchphysical,

    pr.purchreqid,
    prl.linenum purchreq_linenum,
    pl.purchreqlinerefid,   /* this links purchase order line to purchase requisition line (linerefid) */

    pl.dxc_binquantitypo  binquantity,
    pl.dxc_bintype bintype,
    pl.dxc_fishsize fishsize,
    pl.dxc_packagingunit,
    pl.overdeliverypct,
    cur.currencycode purchaseorder_currencycode,
    pl.priceunit,
    pl.underdeliverypct,
    pl.dxc_packagingquantity packagingqty,
    pl.dxc_greenweightconversionfactor greenweightconversionfactor,
    pl.dxc_greenweightquantity greenweightquantity

    /* For Mussel Harvest PO's take Vendor % into account  ticket #56146 */
    /* case 
        when t.vendaccount in ('SF499200', 'SF499201') then coalesce(1 - t.vendor1percent, 1) 
        when t.vendaccount in ('SF499210') then 1
        else coalesce(t.vendor1percent, 1)
        end as vendor1pct_ 
    , case when coalesce(vendor1pct_, 0) = 0 then 1 else vendor1pct_ end as vendor1pct */

    /* ensure GW is correct for Mussel PO's */
    , t.vendor1pct_
    , t.vendor1pct
    , case when t.triptypeid in ('MUSSEL') then round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) * coalesce(ctgw1.conversionfactor, ctgw2.conversionfactor, 1) end as mussel_greenweight_qty

    , case 
        when t.triptypeid in ('MUSSEL') and t.vendaccount in ('SF499200', 'SF499201') then round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) * vendor1pct 
        when t.triptypeid in ('MUSSEL') and t.vendaccount in ('SF499210') then 0 
        else round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) * (1 - vendor1pct)
        end as harvest_vol
    , case when t.triptypeid in ('MUSSEL') and t.vendaccount in ('SF499210') then round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) * vendor1pct else 0 end as internal_purchase_vol
    , case when t.triptypeid in ('MUSSEL') and t.vendaccount not in ('SF499200', 'SF499201', 'SF499210') then round(pl.purchqty * coalesce(uom.conv_factor, uom2.conv_factor, 1), 3) * vendor1pct else 0 end as purchase_vol,
        
    pl.dlvmode,
    pl.dlvterm,
    convert(date, pl.confirmeddlv)                 confirmeddeliverydate,
    convert(date, pl.deliverydate)				 deliverydate,						
    convert(date, pl.depreciationstartdate)        depreciationstartdate,
    convert(date, pl.intrastatfulfillmentdate_hu)  intrastatfulfillmentdate,
    convert(date, pl.servicedate)                  servicedate,
    convert(date, pl.shippingdateconfirmed)        shippingdateconfirmed,
    convert(date, pl.shippingdaterequested)        shippingdaterequested,
    convert(date, pl.modifieddatetime)             modifieddatetime,
    convert(date, pl.createddatetime)              createddatetime,
    convert(date, pl.itmdate)                      itmdate,
    convert(date, pl.itmexfactorydate)             itmexfactorydate,
    convert(date, pl.itmintostoredate)             itmintostoredate,
    convert(date, {{ nullify_invalid_date('pl.dxc_landingdate')}}) as landingdate_,

    convert(date, pt.exchangeratedate)              exchangeratedate,
    cteinv.invoicedate,
    cteinv.currencycode invoice_currencycode,
    cteinv.invoiceqty,
    cteinv.invoicelineamount,

    case when cteinv.purchaseorderline_recid is not null then 1 else 0 end is_invoiced ,

    /* fully invoiced if invoiced qty >= pol qty or invoiced amount >= pol lineamount
     use abs value as -ve values exist
     */
    case when cteinv.purchaseorderline_recid is not null 
        then 
            case when abs(cteinv.invoiceqty) >= abs(pl.purchqty) then 1
                when abs(cteinv.invoicelineamount) >= abs(pl.lineamount) and pl.lineamount <> 0 then 1
            else 0
            end
        else 0 end is_fullyinvoiced ,
    /* over invoiced if invoiced qty > pol qty or invoiced amount > pol lineamount */
    case when cteinv.purchaseorderline_recid is not null 
        then 
            case when abs(cteinv.invoiceqty) > abs(pl.purchqty) then 1
                when abs(cteinv.invoicelineamount) > abs(pl.lineamount) and pl.lineamount <> 0 then 1
            else 0
            end
        else 0 end is_overinvoiced ,

    case when ctepackingslip.purchaseorderline_recid is not null then 1 else 0 end is_receipted ,
    case when ctepackingslipmatch.purchaseorderline_recid is not null then 1 else 0 end is_receiptmatched ,

    ctepackingslip.qty packingslipreceipt_qty,
    ctepackingslipmatch.qty packingslipreceiptmatched_qty,

    /* fully receipted if packingslip receipt qty >= pol qty */
    case when ctepackingslip.purchaseorderline_recid is not null and abs(ctepackingslip.qty) >= abs(pl.purchqty) then 1 else 0 end as is_fullyreceipted,
    case when ctepackingslipmatch.purchaseorderline_recid is not null and abs(ctepackingslipmatch.qty) >= abs(pl.purchqty) then 1 else 0 end as is_fullyreceiptmatched,
    /* over receipted packingslip receipt qty > pol qty */
    case when ctepackingslip.purchaseorderline_recid is not null and abs(ctepackingslip.qty) > abs(pl.purchqty) then 1 else 0 end as is_overreceipted,
    case when ctepackingslipmatch.purchaseorderline_recid is not null and abs(ctepackingslipmatch.qty) > abs(pl.purchqty) then 1 else 0 end as is_overreceiptmatched,

    /* links to project commited costs */
    pl.sourcedocumentline,
    /* link to project transactions */
    pl.projtransid,
    pl.[IsDelete] 

    from {{source('fno', 'purchline')}} pl
    join {{source('fno', 'purchtable')}} pt on pl.purchid = pt.purchid 
        and upper(pl.dataareaid) = upper(pt.dataareaid)
        and pt.IsDelete is null
    left join {{source('fno', 'purchreqline')}} prl on pl.purchreqlinerefid = prl.linerefid
    left join {{ref('dim_d365_purchaserequisition')}} pr on prl.purchreqtable = pr.purchaserequisition_recid

    left join {{ref('dim_d365_purchaseorder')}} po on pl.purchid = po.purchid  and  upper(pl.dataareaid) = po.purchaseorder_dataareaid
    left join {{ref('dim_d365_vendor')}} v on pl.vendaccount = v.accountnum  and upper(pl.dataareaid) = upper(v.vendor_dataareaid)
    left join {{ref('dim_d365_vendor')}} vi on po.invoiceaccount = vi.accountnum  and upper(pl.dataareaid) = upper(vi.vendor_dataareaid)
    left join {{ref('dim_d365_vendor')}} vo on po.orderaccount = vo.accountnum  and upper(pl.dataareaid) = upper(vo.vendor_dataareaid)

    left join {{ref('dim_d365_address')}} del_a on pl.deliverypostaladdress = del_a.address_recid
    left join {{ref('dim_d365_country')}} del_country on del_a.countryregionid = del_country.countryregionid

    left join {{ref('dim_d365_category')}}  cat on pl.procurementcategory = cat.category_recid 
    left join {{ref('dim_d365_item')}}  i on pl.itemid = i.itemid  and  upper(pl.dataareaid) = Upper(i.item_dataareaid)
    left join {{ref('dim_d365_warehouse')}} w on po.warehouseid = w.warehouseid and upper(pl.dataareaid) = w.warehouse_dataareaid
    left join {{ref('dim_d365_site')}} s on w.warehouse_dataareaid = s.site_dataareaid
        and w.siteid = s.siteid
    left join {{ref('dim_d365_currency')}}  cur on po.currencycode = cur.currencycode
    left join {{ref('dim_d365_itemgroup')}}  ig on i.itemgroupid = ig.itemgroupid and upper(pl.dataareaid) = ig.itemgroup_dataareaid
    left join {{ref('dim_d365_itemcoveragegroup')}}  icg on i.itemcoveragegroupid = icg.itemcoveragegroupid and upper(pl.dataareaid)  = icg.itemcoveragegroup_dataareaid
    left join {{ref('dim_d365_itembuyergroup')}}  ibg on i.itembuyergroupid = ibg.itembuyergroupid and upper(pl.dataareaid)  = ibg.itembuyergroup_dataareaid
    left join {{ref('dim_d365_itemstoragedimensiongroup')}}  istoragedim on i.itemstoragedimensiongroupid = istoragedim.itemstoragedimensiongroupid 
    left join {{ref('dim_d365_company')}} co on upper(pl.dataareaid) = co.company_dataarea 
    left join {{ref('stg_fct_d365_consolidationcompanies_1')}} ctecomp on co.dim_d365_company_sk = ctecomp.dim_d365_company_origin_sk
    left join {{ref('dim_d365_enum')}} ept on lower(ept.enumname) = 'purchasetype' and ept.enum_item_value = pl.purchasetype
    left join {{ref('dim_d365_enum')}} epts on lower(epts.enumname) = 'purchstatus' and epts.enum_item_value = pl.purchstatus
    left join {{ref('dim_d365_enum')}} bc on lower(bc.enumname) = 'barcodetype' and bc.enum_item_name = pl.barcodetype
    /* correct join to financial dimension is defaultdim to valueset, ledgerdim to account
     default does not contain main account in 95% of records, so use account dim
     */
    left join {{ref('dim_d365_financialdimensionvalueset')}} fd on pl.defaultdimension = fd.financialdimensionvalueset_recid  
    left join {{ref('dim_d365_financialdimensionaccount')}} fa on pl.ledgerdimension = financialdimensionaccount_recid

    left join {{ref('dim_d365_businessunit')}} bu on fd.d1_businessunit = bu.businessunitid
    left join {{ref('dim_d365_costcenter')}} cc on fd.d2_costcenter = cc.costcenterid
    left join {{ref('dim_d365_department')}} dep on fd.d3_department = dep.departmentid
    left join {{ref('dim_d365_function')}} fun on fd.d4_function= fun.functionid

    left join {{ref('dim_date')}} dt on convert(date, po.deliverydate) = convert(date, dt.calendar_date)
    
    /* left join {{ref('stg_fct_d365_itemposting_1')}} p on i.dim_d365_item_sk = p.dim_d365_item_sk and upper(pl.dataareaid) = upper(p.dataareaid) */
    
    /* Item to ItemPosting use InventAccountType 3 */
    left join {{ref('fct_d365_itemposting_all')}} ip_all
        on 
            ip_all.postinggroup = 'Purchase order'
            and ip_all.inventaccounttype = 3
            and i.dim_d365_item_sk = ip_all.dim_d365_item_sk
            and /* ALL */
                (ip_all.custvendcode = 2)

    /* Category to ItemPosting - non-Item lines */
    left join {{ref('fct_d365_itemposting_all')}} ip_cat
        on 
            ip_cat.postinggroup = 'Purchase order'
            and ip_cat.inventaccounttype = 63
            and ip_cat.dim_d365_item_sk is null
            and pl.procurementcategory = ip_cat.categoryrelation
            and upper(pl.dataareaid) = ip_cat.itemposting_dataareaid
            and /* ALL */
                (ip_cat.custvendcode = 2)
            and ip_all.dim_d365_item_sk is null
    
    left join {{ref('dim_d365_inventdim')}} id on pl.inventdimid = id.inventdimid
        and id.inventdimid != 'AllBlank'
    left join {{ref('dim_d365_fishstate')}} fs on i.mpistatecode = fs.fishstatecode and upper(pl.dataareaid) = upper(fs.fishstate_dataareaid)

    left join {{ref('dim_d365_inventbatch')}} ib on id.inventbatchid = ib.inventbatchid
        and pl.itemid = ib.itemid
        and id.inventdim_dataareaid = ib.inventbatch_dataareaid

    /* possible to have a PO from SANA that is linked to a SANF Trip Order? hence not having join on dataareaid */
    left join tripord t on pl.purchid = t.purchid

    left join cteinv on pl.recid = cteinv.purchaseorderline_recid
    left join ctepackingslip on pl.recid = ctepackingslip.purchaseorderline_recid
    left join ctepackingslipmatch on pl.recid = ctepackingslipmatch.purchaseorderline_recid

    left join {{ref('dim_date')}} dti on convert(date, cteinv.invoicedate) = convert(date, dti.calendar_date)
    left join {{ref('dim_date')}} dtl on convert(date, pl.dxc_landingdate) = convert(date, dtl.calendar_date)

    /* UoM conversion by Item code
       UoM by Item first */
    left join {{ref('stg_fct_d365_unitofmeasureconversion')}} uom on 
        upper(pl.purchunit) = uom.from_unit
        and uom.to_unit = 'KG'
        and i.dim_d365_item_sk = uom.dim_d365_item_sk
    /* If no Item specific UoM  */
    left join {{ref('stg_fct_d365_unitofmeasureconversion')}} uom2 on 
        upper(pl.purchunit) = uom2.from_unit
        and uom2.to_unit = 'KG'
        and uom2.dim_d365_item_sk is null
        /* not already found */
        and uom.dim_d365_item_sk is null

    /* Green Weight from PO is not always correct */
    /* gw site not null , and gw site is matching with invent value site */
    left join {{ref('stg_fct_d365_greenweightconversion')}} ctgw1 on 
        i.dim_d365_item_sk = ctgw1.dim_d365_item_sk
        and s.dim_d365_site_sk = ctgw1.dim_d365_site_sk
        and co.dim_d365_company_sk = ctgw1.dim_d365_company_sk
        and t.landingdate between coalesce(ctgw1.fromdate, t.landingdate) and coalesce(ctgw1.todate, t.landingdate)

    /* gw site IS null */
    left join {{ref('stg_fct_d365_greenweightconversion')}} ctgw2 on 
        i.dim_d365_item_sk = ctgw2.dim_d365_item_sk
        and ctgw2.dim_d365_site_sk is null
        and co.dim_d365_company_sk = ctgw2.dim_d365_company_sk
        and t.landingdate between coalesce(ctgw2.fromdate, t.landingdate) and coalesce(ctgw2.todate, t.landingdate)
        and ctgw1.fact_d365_greenweightconversion_sk is null

    left join {{ref('dim_d365_project')}} prj on pt.projid = prj.projid    
        and upper(pt.dataareaid) = prj.project_dataareaid

    left join {{ref('dim_d365_projectcategory')}} prc on pl.projcategoryid = prc.categoryid    
        and upper(pl.dataareaid) = prc.projectcategory_dataareaid

    /* Approver from Workflow at the line level */
    left join {{ref('stg_dim_d365_workflowapproval_1')}} wf on pl.recid = wf.purchline_recid

    where pl.[IsDelete] is null

),

ctpltot as (
    select
        ctpurchline.*,

        /* must take into account priceunit & use lineamount before working out from qty and unit price as this will match D365 */
        case 
            when lineamount <> 0 then lineamount
            when priceunit in (0,1) then purchline_productweight_kg*purchprice
            else purchline_productweight_kg*(purchprice/priceunit)
        end purchprice_linetotal
    from ctpurchline
),

ctpl as (
    select
        ctpltot.*,
        case when ctpltot.purchaseorder_currencycode = 'NZD' then purchprice else purchprice/ex.exchangerate end as  po_purchprice_nzd,
        case 
            when ctpltot.purchaseorder_currencycode = 'NZD' then purchprice_linetotal 
            else purchprice_linetotal/ex.exchangerate 
        end as  po_cur_purchprice_linetotal_nzd,
        case 
            when ctpltot.purchaseorder_currencycode = 'NZD' then invoicelineamount 
            else invoicelineamount/ex_inv.exchangerate 
        end as invoicelineamount_nzd
    from ctpltot
    left join {{ref('stg_fct_d365_defaultexchangerate')}} ex_inv on invoice_currencycode = ex_inv.tocurrencycode and (exchangeratedate >= ex_inv.validfrom and exchangeratedate <= ex_inv.validto) 
    left join {{ref('stg_fct_d365_defaultexchangerate')}} ex on purchaseorder_currencycode = ex.tocurrencycode and (exchangeratedate >= ex.validfrom and exchangeratedate <= ex.validto) 
)

select
    fact_purchaseorderline_sk,
    purchaseorderline_recid,
    dim_d365_purchaserequisition_sk,
    dim_d365_vendor_sk,
    dim_d365_vendor_invoice_sk,
    dim_d365_vendor_order_sk,
    dim_d365_category_sk,
    dim_d365_item_sk,
    dim_d365_warehouse_sk,
    dim_d365_site_sk,
    dim_d365_currency_sk,
    dim_d365_company_sk,
    dim_d365_company_parent_sk,
    dim_d365_purchaseorder_sk,
    dim_d365_triporder_sk,
    dim_d365_project_sk,
    dim_d365_projectcategory_sk,
    dim_d365_itemgroup_sk,
    dim_d365_itemcoveragegroup_sk,
    dim_d365_itembuyergroup_sk,
    dim_d365_itemstoragedimensiongroup_sk,
    dim_d365_enum_purchtype_sk,
    dim_d365_enum_purchstatus_sk,
    dim_d365_enum_barcodetype_sk,
    dim_deliverydate_key,
    dim_invoicedate_key,
    dim_landingdate_key,
    dim_d365_financialdimensionvalueset_sk,
    dim_d365_financialdimensionaccount_sk,
    dim_d365_mainaccount_sk,
    dim_d365_businessunit_sk,
    dim_d365_costcenter_sk,
    dim_d365_department_sk,
    dim_d365_function_sk,
    dim_d365_delivery_address_sk,
    dim_d365_delivery_country_sk,
    dim_d365_inventdim_sk,
    dim_d365_inventbatch_sk,
    dim_d365_fishstate_sk,
    dataareaid,
    purchid,
    barcode,
    itemid,
    linenumber,
    approver,
    orderline_name,
    dxc_packagingunit,
    overdeliverypct,
    remaininventfinancial,
    remaininventphysical,
    remainpurchfinancial,
    remainpurchphysical,
    inventrefid,
    inventreftransid,
    inventtransid,
    purchunit,
    purchaseorder_currencycode,
    round(productweight_in_purchunit,3) productweight_in_purchunit,
    round(purchline_productweight_kg,3) purchline_productweight_kg,
    priceunit,
    underdeliverypct,
    packagingqty,
    greenweightconversionfactor,
    /* use Mussel PO calculated GW */
    round(coalesce(mussel_greenweight_qty, greenweightquantity), 3) as greenweightquantity,
    vendor1pct,
    round(harvest_vol, 3) as harvest_vol,
    round(internal_purchase_vol, 3) as internal_purchase_vol,
    round(purchase_vol, 3) as purchase_vol,
    deliverydate,
    createddatetime,
    confirmeddeliverydate,
    shippingdateconfirmed,
    shippingdaterequested,
    servicedate,
    itmdate,
    itmexfactorydate,
    itmintostoredate,
    landingdate_ as landingdate,
    binquantity,
    bintype,
    fishsize,
    dlvmode,
    dlvterm,
    purchmarkup,

    exchangeratedate,
    round(lineamount,2) lineamount,
    round(purchprice,4) purchprice_lcy,
    round(po_purchprice_nzd,4) po_purchprice_nzd,
    round(purchprice_linetotal,2) purchprice_linetotal_lcy,
    round(po_cur_purchprice_linetotal_nzd,2) po_cur_purchprice_linetotal_nzd,

    purchreqid,
    round(purchreq_linenum, 0) purchreq_linenum,
    purchreqlinerefid,
    invoicedate,
    round(invoiceqty, 3) invoiceqty,
    round(invoicelineamount, 2) invoicelineamount,
    round(invoicelineamount_nzd, 2) invoicelineamount_nzd,
    is_invoiced,
    is_fullyinvoiced,
    is_overinvoiced,

    is_receipted,
    is_receiptmatched,
    packingslipreceipt_qty,
    packingslipreceiptmatched_qty,
    is_fullyreceipted,
    is_fullyreceiptmatched,
    is_overreceipted,
    is_overreceiptmatched,

    sourcedocumentline,
    projtransid,
    [IsDelete]
    
    from ctpl
    