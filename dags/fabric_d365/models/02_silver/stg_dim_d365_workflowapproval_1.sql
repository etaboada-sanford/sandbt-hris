{{ config
(
materialized= 'incremental',
unique_key= ['dim_d365_workflowapproval_sk']
)
}}

/* Not intended to be an actual dimension table, required for Facts PurchaseOrderLine, PurchaseRequisitionLine & VendPackingSlipLine */

with ct as (
    select
        {{ dbt_utils.generate_surrogate_key(['wtst.recid', 'wtt.recid']) }} as dim_d365_workflowapproval_sk
        -- { { d b t _ utils.generate_surrogate_key(['wtst.recid', 'wtt.recid', 'pprwt.mserp_recid', 'pprwl.recid'])}} as dim_d365_workflowapproval_sk
        , wtst.instancenumber
        /* use to join to Customer, vendor, bank acct if required
            contexttableid = 67286 links to cutom dxc table for Purch line & purch req line */
        , wtst.contexttableid
        , wtst.contextrecid
        , wtst.document
        , wtst.documenttype
        , wtst.originator
        , {{ translate_enum('ewtt', 'wtt.trackingtype' ) }} as trackingtype
        , {{ translate_enum('ewtc', 'wtt.trackingcontext' ) }} as trackingcontext
        , case when wtt.[user] = 'D365BatchUser' then 'D365BatchUser' else p.name end as approver
        /* Admin or Batch user can have approval record after actual user - prioritise actual user
            multiple instancenumbers can exist per contextrecid when not linked to ods_d365_dxc_purchpurchreqworkflowtable
            when this stg dim is used to join on contexttableid & contextrecid ensure that approval_context_rnk =1 in join clause
        */
        , rank() over (
            partition by wtst.instancenumber, wtst.contextrecid
            order by
                case when wtt.[user] in ('D365BatchUser', 'Admin') then 1 else 0 end
                , wtt.createddatetime desc
        ) as approval_rnk
        , rank() over (
            partition by wtst.contexttableid, wtst.contextrecid
            order by
                case when wtt.[user] in ('D365BatchUser', 'Admin') then 1 else 0 end
                , wtt.createddatetime desc
        ) as approval_context_rnk

        , apdnz.newzealandtime as approvaldatetime_nzt
        --    , pprwt.mserp_documentnum
        , null as documentnum
        --    , pprwt.mserp_submitter
        , null as submitter
        --    , pprwt.mserp_createdby
        , null as createdby
        --    , pprwt.mserp_costcentreowner
        , null as costcentreowner
        --    , pprwt.mserp_costcentrevalue
        , null as costcentrevalue
        --    , pprwt.mserp_costcentrename
        , null as costcentrename
        --    , round(pprwt.mserp_costcentreownerapprovalamount, 2) as costcentreownerapprovalamount
        , null as costcentreownerapprovalamount
        --    , pprwt.mserp_budgetcontrol
        , null as budgetcontrol
        --    , eppws.[LocalizedLabel] as workflowstatus
        , null as workflowstatus
        --    , eppwls.[LocalizedLabel] as workflowstatus_line
        , null as workflowstatus_line

        --    , po.purchid
        , null as purchid
        --    , po.recid as purch_recid
        , null as purch_recid
        --    , pol.recid as purchline_recid
        , null as purchline_recid
        --    , pr.purchreqid
        , null as purchreqid
        --    , pr.recid as purchreq_recid
        , null as purchreq_recid
        --    , prl.recid as purchreqline_recid
        , null as purchreqline_recid

        , upper(wtst.contextcompanyid) as dataareaid

    from {{ source('fno', 'workflowtrackingstatustable') }} as wtst
    left join {{ source('fno', 'workflowtrackingtable') }} as wtt on wtst.recid = wtt.workflowtrackingstatustable

    cross apply dbo.f_get_enum_translation('workflowtrackingtable', '1033') as ewtt
    cross apply dbo.f_get_enum_translation('workflowtrackingtable', '1033') as ewtc
    /*
    left join { {s o u rce('mserp', 'dxc_purchpurchreqworkflowtable')}} pprwt on wtst.contexttableid = 67286
        and wtst.contextrecid = pprwt.mserp_recid

    left join { {s o u rce('fno', 'GlobalOp  tionsetM etadata') }} as eppws on eppws.[EntityName] = 'dxc_purchpurchreqworkflowtable'
        and eppws.[OptionSetName] = 'mserp_workflowstatus'
        and pprwt.mserp_workflowstatus = eppws.[Option]

    / * TableID 19571 = PurchReq, 19637 = PurchOrder * /
    left join { { s o u rce('fno', 'purchtable')}} po on pprwt.mserp_reftableid = 19637
        and pprwt.mserp_refrecid = po.recid
    left join { { s o u rce('fno', 'purchreqtable')}} pr on pprwt.mserp_reftableid = 19571
        and pprwt.mserp_refrecid = pr.recid

    / * PO / PR Lines can be approved by different people in same PO / PR * /
    left join { { s o u rce('fno', 'dxc_purchpurchreqworkflowline')}} pprwl on pprwt.mserp_recid = pprwl.purchpurchreqworkflowtablerecid

    left join { { s o u rce('fno', 'GlobalOptio nsetMe tadata') }} as eppwls on eppwls.[EntityName] = 'dxc_purchpurchreqworkflowline'
        and eppwls.[OptionSetName] = 'mserp_workflowstatus'
        and pprwl.workflowstatus = eppwls.[Option]

    / * TableID 6354 = PurchLine, 4574 = PurchReqLine * /
    left join { { s o u rce('fno', 'purchline')}} pol on pprwl.reftableid = 6354
        and pprwl.refrecid = pol.recid

    left join { { s o u rce('fno', 'purchreqline')}} prl on pprwl.reftableid = 4574
        and pprwl.refrecid = prl.recid
    */
    left join {{ source('fno', 'dirpersonuser') }} as u on wtt.[user] = u.[user]
    left join {{ ref('dim_d365_party') }} as p on u.personparty = p.party_recid
    cross apply dbo.f_convert_utc_to_nzt(wtt.createddatetime) as apdnz
    where
        (
            /* auto Admin approval */
            ({{ translate_enum('ewtt', 'wtt.trackingtype' ) }} = 'Completion' and {{ translate_enum('ewtc', 'wtt.trackingcontext' ) }} = 'Approval')
            or
            /* user approved */
            ({{ translate_enum('ewtt', 'wtt.trackingtype' ) }} = 'Approval' and {{ translate_enum('ewtc', 'wtt.trackingcontext' ) }} = 'Work item')
        )
)

select
    dim_d365_workflowapproval_sk
    , instancenumber
    , contexttableid
    , contextrecid
    , document
    , documenttype
    , originator
    , trackingtype
    , trackingcontext
    , approver
    , approval_context_rnk
    , approvaldatetime_nzt
    , documentnum
    , submitter
    , createdby
    , costcentreowner
    , costcentrevalue
    , costcentrename
    , costcentreownerapprovalamount
    , budgetcontrol
    , workflowstatus
    , workflowstatus_line
    , purchid
    , purch_recid
    , purchline_recid
    , purchreqid
    , purchreq_recid
    , purchreqline_recid
    , dataareaid
    , approval_rnk
from ct
where approval_rnk = 1