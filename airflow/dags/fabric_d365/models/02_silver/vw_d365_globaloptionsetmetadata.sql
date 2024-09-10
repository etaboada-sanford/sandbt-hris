{{ config(materialized = 'view') }}

select [OptionSetName] as optionsetname
    , [Option] as [option]
    , [IsUserLocalizedLabel] as isuserlocalizedlabel
    , [LocalizedLabelLanguageCode] as localizedlabellanguagecode
    , [LocalizedLabel] as localizedlabel
    , [GlobalOptionSetName] as globaloptionsetname
    , [EntityName] as entityname
    , [ExternalValue] as externalvalue
    , [createdonpartition]
from {{ source('fno', 'GlobalOptionsetMetadata') }}