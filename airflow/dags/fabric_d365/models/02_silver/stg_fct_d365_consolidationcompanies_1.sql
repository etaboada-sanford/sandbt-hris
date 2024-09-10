select distinct
        co.dim_d365_company_sk dim_d365_company_origin_sk,
        company_parent.dim_d365_company_sk dim_d365_company_parent_sk,
        co.company_dataarea,
        co.company_name,
        company_parent.company_dataarea company_dataarea_parent,
        company_parent.company_name company_name_parent
    from {{ref('stg_d365_consolidationcompanies')}} h
    left join {{ref('dim_d365_company')}} co on upper(co.company_dataarea) = upper(h.company_dataarea) 
    left join {{ref('dim_d365_company')}} company_parent on upper(company_parent.company_dataarea) = upper(h.consolidated_parent_co_dataarea) 
