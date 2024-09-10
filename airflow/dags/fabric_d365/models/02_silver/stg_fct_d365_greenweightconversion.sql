/* This is NEW  VERSION OF LOGICS - 12 APR 2024 - CONCLUDED 
--  rule 1 - match species code + mpi state from + state to + site  --  - get conversion factor 
-- 	rule 2 - match species code + mpi state from + state to (if site not present) 
-- 	rule 3 - rule 3 if not gw record doesnt have site nor species code , match mpi state - get conversion factor 
--  rule 4 - for items falling outside of above rule  - then 0 
--*** special rule : After applying all above rules, apply rule 5 overriding above rule if species code = 'MLT' then greenweight factor is 0
*/

with itgw1 as (
    /* 
        where the site, species and state match 
        these are special cases, do not exclude these items from other selection criteria
    */
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , st.dim_d365_site_sk
        , st.siteid
        , c.mserp_speciescode as speciescode 
        , c.mserp_statefrom as statefrom
        , c.mserp_stateto as stateto
        , convert(date, c.mserp_fromdate) as fromdate
        , convert(date, c.mserp_todate) as todate  
        , c.mserp_conversionfactor as conversionfactor 
        , upper(c.mserp_dataareaid) as dataareaid
        , 1 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.speciescode = c.mserp_speciescode 
        and it.processstatecode = c.mserp_statefrom 
        and upper(c.mserp_dataareaid) = it.item_dataareaid 
    join  {{ref('dim_d365_site')}} st on c.mserp_inventsiteid = st.siteid and upper(c.mserp_dataareaid) = st.site_dataareaid    
    where upper(c.mserp_dataareaid) = 'SANF'
    )

, itgw2 as (
    /* 
        where the site is NULL, species and state match 
        exclude these cases from subsequent selection criteria
    */
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , c.mserp_speciescode as speciescode
        , c.mserp_statefrom as statefrom
        , c.mserp_stateto as stateto
        , convert(date, c.mserp_fromdate) fromdate
        , convert(date, c.mserp_todate) todate  
        , c.mserp_conversionfactor as conversionfactor 
        , upper(c.mserp_dataareaid) dataareaid
        , 2 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.speciescode = c.mserp_speciescode 
        and it.processstatecode = c.mserp_statefrom 
        and upper(c.mserp_dataareaid) = it.item_dataareaid 
    where upper(c.mserp_dataareaid) = 'SANF'
        and c.mserp_inventsiteid is null
    )  

, itgw3 as (
    /* 
        where the site is NULL, state match 
        excludes these cases from subsequent selection criteria
    */
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , it.speciescode 
        , c.mserp_statefrom as statefrom
        , c.mserp_stateto as stateto
        , convert(date, c.mserp_fromdate) fromdate
        , convert(date, c.mserp_todate) todate  
        , c.mserp_conversionfactor 
        , upper(c.mserp_dataareaid) dataareaid
        , 3 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.processstatecode = c.mserp_statefrom 
        and upper(c.mserp_dataareaid) = it.item_dataareaid
    left join itgw2 on it.dim_d365_item_sk = itgw2.dim_d365_item_sk
    where upper(c.mserp_dataareaid) = 'SANF'
        and c.mserp_inventsiteid is null
        and c.mserp_speciescode is null
        and itgw2.dim_d365_item_sk is null
    )   

, itgw4 as (
    /* 
    rule 4 - for items falling outside of above ruleS  - then 0 
    */
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , it.speciescode 
        , it.mpistatecode as statefrom
        , null as stateto
        , convert(date, '1900-01-01') fromdate
        , convert(date, '1900-01-01') todate  
        , 0 as conversionfactor 
        , it.item_dataareaid dataareaid
        , 4 as source
    from {{ref('dim_d365_item')}} it
    left join itgw2 on it.dim_d365_item_sk = itgw2.dim_d365_item_sk
    left join itgw3 on it.dim_d365_item_sk = itgw3.dim_d365_item_sk
    where it.item_dataareaid = 'SANF'
        and left(it.itemid, 2) in ('10', '20', '30')
        and itgw2.dim_d365_item_sk is null
        and itgw3.dim_d365_item_sk is null
    ) 

, cte_merge as (
    select itgw1.* from itgw1
    union all
    select itgw2.* from itgw2
    union all
    select itgw3.* from itgw3
    union all
    select itgw4.* from itgw4

    )

, cte_final as (
select 
    {{ dbt_utils.generate_surrogate_key(['co.dim_d365_company_sk','dim_d365_item_sk','dim_d365_site_sk','fromdate','todate'])}} as fact_d365_greenweightconversion_sk
    , co.dim_d365_company_sk
    , dim_d365_item_sk
    , dataareaid
    , itemid
    , dim_d365_site_sk
    , speciescode
    , statefrom
    , stateto
    , case when fromdate = '1900-01-01' then null else fromdate end fromdate
    , case when todate = '1900-01-01' then null else todate end todate
    /*, case when speciescode = 'MLT' then 0 else conversionfactor end conversionfactor
    change requested 10/04/2024
    */
    , case 	when left(itemid, 2) = '10' and conversionfactor > 1 then 1
            when left(itemid, 2) = '20' and statefrom <> 'FLW' and conversionfactor > 1 then 1
            when statefrom = 'BLD' then 1
            when statefrom = 'HDS' then 0
            when speciescode = 'MLT' then 0 
		else conversionfactor 
    end conversionfactor
    , source
from cte_merge
join {{ref('dim_d365_company')}}  co on cte_merge.dataareaid = co.company_dataarea
)

select *
from cte_final

/* This is OLD VERSION OF CODES 
--so we are concluding below rule sequence \ steps to check conversion factor for the item
--  rule 1 - match species code + mpi state + site  --  - get conversion factor  
--  rule 2 if gw record doesnt have site then - rule 2 - match species code + mpi state  - - get conversion factor 
--  rule 3 if not gw record doesnt have site nor species code , match mpi state - get conversion factor 
--  rule 4 - for items falling outside of above rule , and if item name has gre in it , then conversion factor = 1

*** RULE 5 : After applying all above rules, apply rule 5 overriding above rule
             if species code = 'MLT' then greenweight factor is 0


with itgw1 as (

    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , st.dim_d365_site_sk
        , st.siteid
        , c.speciescode 
        , c.statefrom
        , c.stateto
        , convert(date, c.fromdate) fromdate
        , convert(date, c.todate) todate  
        , c.conversionfactor 
        , upper(c.dataareaid) dataareaid
        , 1 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.speciescode = c.speciescode 
        and it.processstatecode = c.statefrom 
        and upper(c.dataareaid) = it.item_dataareaid 
    join  {{ref('dim_d365_site')}} st on c.inventsiteid = st.siteid and upper(c.dataareaid) = st.site_dataareaid    
    where upper(c.dataareaid) = 'SANF'
    )

, itgw2 as (
    
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , c.speciescode 
        , c.statefrom
        , c.stateto
        , convert(date, c.fromdate) fromdate
        , convert(date, c.todate) todate  
        , c.conversionfactor 
        , upper(c.dataareaid) dataareaid
        , 2 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.speciescode = c.speciescode 
        and it.processstatecode = c.statefrom 
        and upper(c.dataareaid) = it.item_dataareaid 
    where upper(c.dataareaid) = 'SANF'
        and c.inventsiteid is null
    )  

, itgw3 as (

    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , it.speciescode 
        , c.statefrom
        , c.stateto
        , convert(date, c.fromdate) fromdate
        , convert(date, c.todate) todate  
        , c.conversionfactor 
        , upper(c.dataareaid) dataareaid
        , 3 as source
    from {{source('mserp', 'dxc_greenweightconversion')}} c
    join {{ref('dim_d365_item')}} it on it.processstatecode = c.statefrom 
        and upper(c.dataareaid) = it.item_dataareaid
    left join itgw2 on it.dim_d365_item_sk = itgw2.dim_d365_item_sk
    where upper(c.dataareaid) = 'SANF'
        and c.inventsiteid is null
        and c.speciescode is null
        and itgw2.dim_d365_item_sk is null
    )   

, itgw4 as (
 
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , it.speciescode 
        , 'GRE' as statefrom
        , 'GRE' as stateto
        , convert(date, '1900-01-01') fromdate
        , convert(date, '1900-01-01') todate  
        , 1 as conversionfactor 
        , it.item_dataareaid dataareaid
        , 4 as source
    from {{ref('dim_d365_item')}} it
    left join itgw2 on it.dim_d365_item_sk = itgw2.dim_d365_item_sk
    left join itgw3 on it.dim_d365_item_sk = itgw3.dim_d365_item_sk
    where it.item_dataareaid = 'SANF'
        and left(it.itemid, 2) in ('10', '20', '30')
        and (it.processstatecode = 'GRE' or it.itemid like '%GRE%')
        and itgw2.dim_d365_item_sk is null
        and itgw3.dim_d365_item_sk is null
    ) 

, itgw5 as (
    
    select distinct
        it.dim_d365_item_sk
        , it.itemid
        , null as dim_d365_site_sk
        , null as siteid
        , it.speciescode 
        , processstatecode as statefrom
        , processstatecode as stateto
        , convert(date, '1900-01-01') fromdate
        , convert(date, '1900-01-01') todate  
        , 1 as conversionfactor 
        , it.item_dataareaid dataareaid
        , 5 as source
    from {{ref('dim_d365_item')}} it
    left join itgw2 on it.dim_d365_item_sk = itgw2.dim_d365_item_sk
    left join itgw3 on it.dim_d365_item_sk = itgw3.dim_d365_item_sk
    left join itgw4 on it.dim_d365_item_sk = itgw4.dim_d365_item_sk
    where it.item_dataareaid = 'SANF'
        and left(it.itemid, 2) in ('10', '20', '30')
        and itgw2.dim_d365_item_sk is null
        and itgw3.dim_d365_item_sk is null
        and itgw4.dim_d365_item_sk is null
    )     

, cte_merge as (
    select itgw1.* from itgw1
    union all
    select itgw2.* from itgw2
    union all
    select itgw3.* from itgw3
    union all
    select itgw4.* from itgw4
    union all
    select itgw5.* from itgw5
    )

, cte_final as (
select 
    {{ dbt_utils.generate_surrogate_key(['co.dim_d365_company_sk','dim_d365_item_sk','dim_d365_site_sk','fromdate','todate'])}} as fact_d365_greenweightconversion_sk
    , co.dim_d365_company_sk
    , dim_d365_item_sk
    , dataareaid
    , itemid
    , dim_d365_site_sk
    , speciescode
    , statefrom
    , stateto
    , case when fromdate = '1900-01-01' then null else fromdate end fromdate
    , case when todate = '1900-01-01' then null else todate end todate
  
    , case 	when speciescode = 'MLT' then 0 
			when  speciescode = 'SAM' and statefrom = 'OFF' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'BEL' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'CMK' and stateto = 'GRE' then  2.86
			when  speciescode = 'SAM' and statefrom = 'COL' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'FRA' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'PCS' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'HDS' and stateto = 'GRE' then  0
			when  speciescode = 'SAM' and statefrom = 'HMK' and stateto = 'GRE' then  2.86
		else conversionfactor 
    end conversionfactor
    , source
from cte_merge
join {{ref('dim_d365_company')}}  co on cte_merge.dataareaid = co.company_dataarea
)

select *
from cte_final

*/