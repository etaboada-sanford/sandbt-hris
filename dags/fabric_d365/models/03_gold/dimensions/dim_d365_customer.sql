{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_customer_sk']
) }}

with ctcust as (
    select
        c.[Id] as dim_d365_customer_sk
        , c.recid as customer_recid
        , c.salesdistrictid

        , sdg.description as salesdistrictgroupdesc
        , sdg.dataareaid as sdgdataareaid

        , c.party
        , c.accountnum
        , coalesce(c.invoiceaccount, c.accountnum) as invoiceaccount
        , c.ouraccountnum
        , c.creditmax
        , c.custgroup
        , upper(c.dataareaid) as dataareaid
        , c.dlvmode
        , c.dlvterm
        , c.lineofbusinessid
        , c.paymmode
        , c.pricegroup

        , c.salespoolid
        , c.segmentid
        , sgm.description as segment_description
        , c.statisticsgroup
        , c.subsegmentid
        , c.currency as customer_currency
        , c.partycountry
        , c.partystate
        , c.orgid

        , pt.recid as party_recid
        , pt.isactive

        , coy.coregnum
        , pt.partynumber
        , pt.orgnumber
        , pt.name
        , pt.namealias
        , pt.namesequence
        , pt.namecontrol
        , pt.primarycontactphone
        , pt.primaryaddresslocation
        , pt.numberofemployees
        , pt.dataarea
        , pt.createddatetime
        , pt.createdby
        , pt.modifieddatetime
        , pt.modifiedby
        , c.partition
        , c.blocked as blockedid
        , {{ translate_enum('custtable_enum', 'c.blocked' ) }} as blocked

        , c.defaultdimension as defaultdimension_recid
        , c.[IsDelete]
        , c.versionnumber
        , c.sysrowversion
    from {{ source('fno', 'custtable') }} as c
    left join {{ source('fno', 'vw_dirpartytable') }} as pt on c.party = pt.recid
    left join {{ source('fno', 'companyinfo') }} as coy on c.dataareaid = coy.dataareaid
    left join {{ source('fno', 'smmbusrelsalesdistrictgroup') }} as sdg on c.salesdistrictid = sdg.salesdistrictid and upper(c.dataareaid) = upper(sdg.dataareaid)
    left join {{ source('fno', 'smmbusrelsegmentgroup') }} as sgm on c.segmentid = sgm.segmentid and upper(c.dataareaid) = upper(sgm.dataareaid)
    cross apply dbo.f_get_enum_translation('custtable', '1033') as custtable_enum
    {%- if is_incremental() %}
        where c.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where c.[IsDelete] is null
    {% endif %}
)

, custfinal as (
    select
        d365cg.dim_d365_customer_sk
        , d365cg.customer_recid
        , d365cg.accountnum
        , d365cg.invoiceaccount
        , d365cg.ouraccountnum
        , d365cg.orgnumber
        , d365cg.name
        , d365cg.party as party_recid
        , d365cg.custgroup
        , d365cg.salesdistrictid
        , d365cg.salesdistrictgroupdesc
        , d365cg.blocked
        , d365cg.pricegroup
        , d365cg.defaultdimension_recid
        , d365cg.dataareaid as customer_dataareaid
        , d365cg.[IsDelete]
        , d365cg.versionnumber
        , d365cg.sysrowversion
    from ctcust as d365cg
    union
    select
        navcg.dim_d365_customer_sk
        , navcg.customer_recid
        , navcg.accountnum
        , navcg.invoiceaccount
        , navcg.ouraccountnum
        , navcg.orgnumber
        , navcg.name
        , navcg.party_recid
        , navcg.custgroup
        , navcg.salesdistrictid
        , navcg.salesdistrictgroupdesc
        , navcg.blocked
        , navcg.pricegroup
        , navcg.defaultdimension_recid
        , navcg.customer_dataareaid
        , navcg.[IsDelete]
        , navcg.versionnumber
        , navcg.sysrowversion
    from {{ ref('stg_nav_dummy_custgroup') }} as navcg
)

select
    cu.*
    , pcu.name as parent_customer_name
    , cu.invoiceaccount as parent_customer_account
from custfinal as cu
left join custfinal as pcu on cu.invoiceaccount = pcu.accountnum and cu.customer_dataareaid = pcu.customer_dataareaid
