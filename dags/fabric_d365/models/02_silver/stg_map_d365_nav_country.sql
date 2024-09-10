select
    [D365_COUNTRY_CODE] as d365_country_code
    , [D365_COUNTRY_NAME] as d365_country_name
    , [D365_COUNTRY_CONCAT] as d365_country_concat
    , [D365_SALES_DISTRICT] as d365_sales_district
    , [D365_DISTRICT_STATISTICS_GROUP] as d365_district_statistics_group
    , [D365_CONTINENT] as d365_continent
    , [D365_PLANNING_GROUP] as d365_planning_group
    , [TM1_PLANNING_GROUP] as tm1_planning_group
    , [TM1_CONTINENT] as tm1_continent
    , [TM1_COUNTRY_NAME_HISTORICAL] as tm1_country_name_historical
    , [TM1_HISTORICAL_COUNTRY] as tm1_historical_country
    , [MAPPING_SUBREGION] as mapping_subregion
    , [MAPPING_COUNTRY] as mapping_country
    , row_number() over (partition by 0 order by [D365_COUNTRY_CODE]) as map_d365_nav_country_sk
from {{ source('stage', 'country_mapping') }}
