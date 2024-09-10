
/* Get recids for tables involved in invoice to receipt to po to purch req linking
-- start at vendpackingsliptrans as lines may not be linked to VendInvoiceInfoSubLine

-- unique rec id is ods_d365_vendinvoicepackingslipquantitymatch
-- however this may not be the case when ods_d365_vendinvoicepurchlink used
-- see vipl_recid in (5637755340, 5637755341)

-- so when using this table get the distinct recids need to join out to other tables

e.g.
-- purchase order lines to 'receipt' matched quantity
-- distinct needed as vendinvoicepackingslipquantitymatch recids may not be unique in stg_fct_d365_vendorinvoicematching_1
ctepackingslipmatch as (
    select pl.recid purchaseorderline_recid
        , round(sum(vipsqm.quantity), 3) qty
    from {{source('fno', 'purchline')}} pl
    join (
        select distinct
            vim.pl_recid
            , vim.vipsqm_recid
        from stg_fct_d365_vendorinvoicematching_1 vim
        ) vim on pl.recid = vim.pl_recid
    join {{source('fno', 'vendinvoicepackingslipquantitymatch')}} vipsqm on vipsqm.recid = vim.vipsqm_recid
    group by pl.recid
    )
*/

with recids as (
    select 
    vpsl.recid vpsl_recid
    , vpsj.recid vpsj_recid
    , vipsqm.recid vipsqm_recid
    , vit.recid vit_recid
    , vij.recid vij_recid
    , vipl.recid vipl_recid
    , viisl.recid viisl_recid
    , viil.recid viil_recid
    , pl.recid pl_recid
    , prl.recid prl_recid
    
    , viisl.parmid viisl_parmid
    , vij.parmid vij_parmid
    
from {{source('fno', 'vendpackingsliptrans')}} vpsl 
join {{source('fno', 'vendpackingslipjour')}} vpsj on vpsl.vendpackingslipjour = vpsj.recid 
left join {{source('fno', 'vendinvoicepackingslipquantitymatch')}} vipsqm on vpsl.sourcedocumentline = vipsqm.packingslipsourcedocumentline
left join {{source('fno', 'vendinvoicetrans')}} vit on vipsqm.invoicesourcedocumentline = vit.sourcedocumentline
left join {{source('fno', 'vendinvoicepurchlink')}} vipl on vit.invoiceid = vipl.invoiceid
    and vit.internalinvoiceid = vipl.internalinvoiceid
    
left join {{source('fno', 'vendinvoiceinfosubline')}} viisl on viisl.journalreftableid = 9879
    and vpsl.recid = viisl.journalrefrecid
left join {{source('fno', 'vendinvoiceinfoline')}} viil on viisl.linerefrecid = viil.recid

left join {{source('fno', 'vendinvoicejour')}} vij on vit.invoiceid = vij.invoiceid
    and vit.internalinvoiceid = vij.internalinvoiceid

left join {{source('fno', 'purchline')}} pl on viil.purchlinerecid = pl.recid
left join {{source('fno', 'purchreqline')}} prl on pl.purchreqlinerefid = prl.linerefid

    
where (viisl.parmid is null or viisl.parmid = vij.parmid)
)

select recs.*
from recids recs
where not exists (
    select 1
    from recids r
    where 
        recs.vpsl_recid = r.vpsl_recid
        and recs.vpsj_recid = r.vpsj_recid
        and recs.vipsqm_recid = r.vipsqm_recid
        and recs.vit_recid = r.vit_recid
        and recs.vij_recid = r.vij_recid
        and recs.vipl_recid = r.vipl_recid
        /* vendinvoiceinfosublines are diff */
        and recs.viisl_recid != r.viisl_recid
        /* keep the one that matches on parmid */
        and r.viisl_parmid is not null
    )