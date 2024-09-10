{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_enum_sk']
) }}

select
    {{ dbt_utils.generate_surrogate_key(['[OptionSetName]','[ExternalValue]','[LocalizedLabel]','[Option]']) }} as dim_d365_enum_sk
    , [EntityName]
    , [OptionSetName]
    , concat([EntityName], [OptionSetName]) as enumname
    , [ExternalValue] as enum_item_name
    , [Option] as enum_item_value
    , [LocalizedLabel] as enumitemlabel
    , [IsUserLocalizedLabel]
    , [LocalizedLabelLanguageCode]
    , [GlobalOptionSetName]
    , [createdonpartition]
    , null as [IsDelete]
from {{ source('fno', 'GlobalOptionsetMetadata') }}
