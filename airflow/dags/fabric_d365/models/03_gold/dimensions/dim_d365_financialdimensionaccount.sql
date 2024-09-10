{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_financialdimensionaccount_sk']
) }}

select
    *
    , 'gl_category_l1' as gl_category_l1
    , 'gl_category_l2' as gl_category_l2
    , 'gl_category_l3' as gl_category_l3
    , concat(
        coalesce(mainaccountname, '')
        , '-', coalesce(d1_businessunit_name, '')
        , '-', coalesce(d2_costcenter_name, '')
        , '-', coalesce(d4_function_name, '')
        , '-', coalesce(d3_department_name, '')
        , '-', coalesce(d5_customer_name, '')
        , '-', coalesce(d6_vendor_name, '')
        , '-', coalesce(d7_projectvalue, '')
        , '-', coalesce(d8_legalentityvalue, '')
    ) as dimension_ledgeraccount_text

from (
    select
        d.[Id] as dim_d365_financialdimensionaccount_sk
        , d.recid as financialdimensionaccount_recid
        , d.mainaccount
        , d.mainaccountvalue
        , m.name as mainaccountname
        , d.d1_businessunit
        , d.d1_businessunitvalue
        , d1pt.name as d1_businessunit_name
        , d.d2_costcenter

        , d.d2_costcentervalue
        , d2pt.name as d2_costcenter_name
        , d.d4_function

        , d.d4_functionvalue
        , d4.description as d4_function_name
        , d.d3_department

        , d.d3_departmentvalue
        , d3.description as d3_department_name
        , d.d5_customer



        , d.d5_customervalue
        , d5pt.name as d5_customer_name
        , d.d6_vendor

        , d.d6_vendorvalue
        , d6pt.name as d6_vendor_name
        , d.d7_project

        , d.d7_projectvalue
        , d.d8_legalentity
        , d.d8_legalentityvalue
        , d.partition
        , d.[IsDelete]
        , d.versionnumber
        , d.sysrowversion
        , concat(
            coalesce(d.mainaccountvalue, '')
            , '-', coalesce(d.d1_businessunitvalue, '')
            , '-', coalesce(d.d2_costcentervalue, '')
            , '-', coalesce(d.d4_functionvalue, '')
            , '-', coalesce(d.d3_departmentvalue, '')
            , '-', coalesce(d.d5_customervalue, '')
            , '-', coalesce(d.d6_vendorvalue, '')
            , '-', coalesce(d.d7_projectvalue, '')
            , '-', coalesce(d.d8_legalentityvalue, '')
        ) as dimension_ledgeraccount
    from {{ source('fno', 'dimensionattributevaluecombination') }} as d
    left join {{ source('fno', 'mainaccount') }} as m on d.mainaccount = m.recid
    left join {{ source('fno', 'omoperatingunit') }} as d1 on d.d1_businessunit = d1.recid
    left join {{ source('fno', 'dirpartytable') }} as d1pt on convert(varchar, d1.recid) = convert(varchar, d1pt.recid)
    left join {{ source('fno', 'omoperatingunit') }} as d2 on d.d2_costcenter = d2.recid
    left join {{ source('fno', 'dirpartytable') }} as d2pt on convert(varchar, d2.recid) = convert(varchar, d2pt.recid)
    left join {{ source('fno', 'dimensionfinancialtag') }} as d3 on d.d3_department = d3.recid
    left join {{ source('fno', 'dimensionfinancialtag') }} as d4 on d.d4_function = d4.recid
    left join {{ source('fno', 'custtable') }} as d5 on d.d5_customer = d5.recid
    left join {{ source('fno', 'dirpartytable') }} as d5pt on convert(varchar, d5.party) = convert(varchar, d5pt.recid)
    left join {{ source('fno', 'vendtable') }} as d6 on d.d6_vendor = d6.recid
    left join {{ source('fno', 'dirpartytable') }} as d6pt on convert(varchar, d6.party) = convert(varchar, d6pt.recid)
    {%- if is_incremental() %}
        where d.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where d.[IsDelete] is null
    {% endif %}
) as x
