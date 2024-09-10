{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_category_sk'],
    tags = ['pyspark_generated_source']
) }}

{%- set levels = [1, 2, 3, 4, 5, 6, 7] %}

with ctcat as (
    select
        c.[Id] as dim_d365_category_sk
        , c.category_recid
        , c.parentcategory
        , c.category_level
        , c.category_name
        , c.categoryhierarchy
        , c.category_path
        , c.sysrowversion
        , c.versionnumber
        , case when c.[IsDelete] = 0 then null else c.[IsDelete] end as [IsDelete]
    from {{ source('stage', 'stg_d365_category') }} as c
    {%- if is_incremental() %}
        where c.sysrowversion > {{ get_max_sysrowversion() }}
    {% else %}
        where c.[IsDelete] null or c.[IsDelete] = 0
    {% endif %}

)

, ctcat1 as (
    select
        ctcat.*
        , pt.name as parentcategory_name
        , cath.name as category_hierarchy
        , replace(ctcat.category_path, ' -> ', '*') as category_path_level
    from ctcat as ctcat
    left join {{ source('fno', 'ecorescategory') }} as pt on ctcat.parentcategory = pt.recid
    left join {{ source('fno', 'ecorescategoryhierarchy') }} as cath on ctcat.categoryhierarchy = cath.recid
)

, ctsplit as (
    select
        splitdata.dim_d365_category_sk
        , splitdata.category_path_level
        {% for level in levels -%}
            ,
            max(case when splitdata.rn = {{ level }} then splitdata.cat_value end) as category_path_level_{{ level }}
        {%- endfor %}
    from (
        select
            ctcat1.dim_d365_category_sk
            , ctcat1.category_path
            , ctcat1.category_path_level
            , value as cat_value -- noqa: RF02
            , row_number() over (partition by ctcat1.dim_d365_category_sk order by (select null as nill)) as rn
        from ctcat1 as ctcat1
        cross apply string_split(ctcat1.category_path_level, '*')
    ) as splitdata
    group by splitdata.dim_d365_category_sk, splitdata.category_path_level
)

select
    ctcat1.dim_d365_category_sk
    , ctcat1.category_recid
    , ctcat1.parentcategory
    , ctcat1.category_level
    , ctcat1.category_name
    , ctcat1.categoryhierarchy
    , ctcat1.category_path
    , ctcat1.parentcategory_name
    , ctcat1.category_hierarchy
    , ctcat1.category_path_level
    , ctcat1.[IsDelete]
    , ctcat1.sysrowversion
    , ctcat1.versionnumber
    {% for level in levels -%}
        , ctsplit.category_path_level_{{ level }}
    {% endfor -%}  
from ctcat1
inner join ctsplit on ctcat1.dim_d365_category_sk = ctsplit.dim_d365_category_sk
union all
select
    {{ dbt_utils.generate_surrogate_key(['-999999']) }} as dim_d365_category_sk
    , -999999 as category_recid
    , -1 as parentcategory
    , -1 as category_level
    , '' as category_name
    , -1 as categoryhierarchy
    , '' as category_path
    , '' as parentcategory_name
    , '' as category_hierarchy
    , '' as category_path_level
    , null as [IsDelete]
    , 0 as sysrowversion
    , 0 as versionnumber
    {% for level in levels -%}
        , null as category_path_level_{{ level }}
        {% endfor %}
