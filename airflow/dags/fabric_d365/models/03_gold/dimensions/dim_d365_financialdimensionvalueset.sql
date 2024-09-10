{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_financialdimensionvalueset_sk']
) }}

with dimattrib as (
    select
        d.[Id] as dim_d365_financialdimensionvalueset_sk
        , d.recid as financialdimensionvalueset_recid
        , d.mainaccount
        , d.mainaccountvalue
        , m.name as mainaccountname
        , d.d1_businessunit
        , d.d1_businessunitvalue
        , d1pt.d1_businessunit_name
        , d.d2_costcenter

        , d.d2_costcentervalue
        , d2pt.d2_costcenter_name
        , d.d4_function

        , d.d4_functionvalue
        , d4.d4_function_name
        , d.d3_department

        , d.d3_departmentvalue
        , d3.d3_department_name
        , d.d5_customer

        , d.d5_customervalue
        , d5pt.d5_customer_name
        , d.d6_vendor

        , d.d6_vendorvalue
        , d6pt.d6_vendor_name
        , d.d7_project

        , d.d7_projectvalue
        , d.d8_legalentity

        , d.d8_legalentityvalue
        , d.partition

        , d.[IsDelete]
        , d.versionnumber
        , d.sysrowversion
        , case
            when
                len(coalesce(d.mainaccountvalue, '')) > 0
                then concat(
                        coalesce(d.mainaccountvalue, '')
                        , '-'
                        , coalesce(d.d1_businessunitvalue, '')
                        , '-'
                        , coalesce(d.d2_costcentervalue, '')
                        , '-'
                        , coalesce(d.d4_functionvalue, '')
                        , '-'
                        , coalesce(d.d3_departmentvalue, '')
                        , '-'
                        , coalesce(d.d5_customervalue, '')
                        , '-'
                        , coalesce(d.d6_vendorvalue, '')
                        , '-'
                        , coalesce(d.d7_projectvalue, '')
                        , '-'
                        , coalesce(d.d8_legalentityvalue, '')
                    )
            when
                len(coalesce(d.mainaccountvalue, '')) = 0
                then concat(
                        coalesce(d.d1_businessunitvalue, '')
                        , '-'
                        , coalesce(d.d2_costcentervalue, '')
                        , '-'
                        , coalesce(d.d4_functionvalue, '')
                        , '-'
                        , coalesce(d.d3_departmentvalue, '')
                        , '-'
                        , coalesce(d.d5_customervalue, '')
                        , '-'
                        , coalesce(d.d6_vendorvalue, '')
                        , '-'
                        , coalesce(d.d7_projectvalue, '')
                        , '-'
                        , coalesce(d.d8_legalentityvalue, '')
                    )
        end as dimension_ledgeraccount
    from {{ source('fno', 'dimensionattributevalueset') }} as d
    left join {{ source('fno', 'mainaccount') }} as m on d.mainaccount = m.recid
    left join {{ ref('stg_d365_davs_businessunit') }} as d1pt on d.recid = d1pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_costcenter') }} as d2pt on d.recid = d2pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_function') }} as d4 on d.recid = d4.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_department') }} as d3 on d.recid = d3.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_customer') }} as d5pt on d.recid = d5pt.financialdimensionvalueset_recid
    left join {{ ref('stg_d365_davs_vendor') }} as d6pt on d.recid = d6pt.financialdimensionvalueset_recid
    {%- if is_incremental() %}
        where d.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where d.[IsDelete] is null
    {% endif %}
)

, dal as (
    select
        *
        , concat(
            coalesce(mainaccountname, '')
            , '-'
            , coalesce(d1_businessunit_name, '')
            , '-'
            , coalesce(d2_costcenter_name, '')
            , '-'
            , coalesce(d4_function_name, '')
            , '-'
            , coalesce(d3_department_name, '')
            , '-'
            , coalesce(d5_customer_name, '')
            , '-'
            , coalesce(d6_vendor_name, '')
            , '-'
            , coalesce(d7_projectvalue, '')
            , '-'
            , coalesce(d8_legalentityvalue, '')
        ) as dimension_ledgeraccount_text
    from dimattrib
)

select
    dal.dim_d365_financialdimensionvalueset_sk
    , dal.financialdimensionvalueset_recid
    , dal.dimension_ledgeraccount
    , dal.mainaccount
    , dal.mainaccountvalue
    , dal.mainaccountname
    , dal.d1_businessunit
    , dal.d1_businessunitvalue
    , dal.d1_businessunit_name
    , dal.d2_costcenter
    , dal.d2_costcentervalue
    , dal.d2_costcenter_name
    , dal.d4_function
    , dal.d4_functionvalue
    , dal.d4_function_name
    , dal.d3_department
    , dal.d3_departmentvalue
    , dal.d3_department_name
    , dal.d5_customer
    , dal.d5_customervalue
    , dal.d5_customer_name
    , dal.d6_vendor
    , dal.d6_vendorvalue
    , dal.d6_vendor_name
    , dal.d7_project
    , dal.d7_projectvalue
    , dal.d8_legalentity
    , dal.d8_legalentityvalue
    , dal.partition
    , dal.dimension_ledgeraccount_text
    , dal.[IsDelete]
    , dal.versionnumber
    , dal.sysrowversion
from dal
union all
select
    fdv.dim_d365_financialdimensionvalueset_sk
    , fdv.financialdimensionvalueset_recid
    , fdv.dimension_ledgeraccount
    , fdv.mainaccount
    , fdv.mainaccountvalue
    , fdv.mainaccountname
    , fdv.d1_businessunit
    , fdv.d1_businessunitvalue
    , fdv.d1_businessunit_name
    , fdv.d2_costcenter
    , fdv.d2_costcentervalue
    , fdv.d2_costcenter_name
    , fdv.d4_function
    , fdv.d4_functionvalue
    , fdv.d4_function_name
    , fdv.d3_department
    , fdv.d3_departmentvalue
    , fdv.d3_department_name
    , fdv.d5_customer
    , fdv.d5_customervalue
    , fdv.d5_customer_name
    , fdv.d6_vendor
    , fdv.d6_vendorvalue
    , fdv.d6_vendor_name
    , fdv.d7_project
    , fdv.d7_projectvalue
    , fdv.d8_legalentity
    , fdv.d8_legalentityvalue
    , fdv.partition
    , fdv.dimension_ledgeraccount_text
    , fdv.[IsDelete]
    , fdv.versionnumber
    , fdv.sysrowversion
from
    {{ ref('stg_nav_financialdimensionvalueset') }} as fdv
