{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_company_sk'],
    tags = ['pyspark_generated_source']
) }}

select
    comp.[Id] as dim_d365_company_sk
    , comp.company_recid
    , comp.party_recid
    , comp.parentorganization
    , comp.company_dataarea
    , comp.parent_co_dataarea
    , comp.company
    , comp.company_name
    , comp.parent_company_name
    , comp.company_path
    , comp.coregnum
    , comp.bank
    , comp.hierarchytypename
    , comp.company_path_level
    , comp.versionnumber
    , comp.sysrowversion
    , case when [IsDelete] = 'false' then null else [IsDelete] end as [IsDelete]
from {{ source('stage', 'stg_d365_company') }} as comp
{%- if is_incremental() %}
    where sysrowversion > {{ get_max_sysrowversion() }}
{% else %}
    where [IsDelete] = 'false'
{% endif %}
