{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_loadtemplate_sk']
) }}

/* LoadTemplate for TEU's */
select
    wlt.[Id] as dim_d365_loadtemplate_sk
    , wlt.recid as loadtemplate_recid
    , wlt.loadtemplateid
    , wlt.equipmentcode
    , wlt.loadheight
    , wlt.loaddepth
    , wlt.loadwidth
    , wlt.loadmaxvolume
    , wlt.loadmaxweight
    /* ISO Equipmentcode determines length
    -- 1st digit 2 = 20ft, 4 = 40ft
    -- 2nd digit is height
    -- chars 3 & 4 are type G general purpose, R refrigerated, T Tank
    */
    , case
        when left(wlt.equipmentcode, 1) = '2' then 1
        when left(wlt.equipmentcode, 1) = '4' then 2
    end as teu
    , upper(wlt.dataareaid) as loadtemplate_dataareaid
    , wlt.versionnumber
    , wlt.sysrowversion
from {{ source('fno', 'whsloadtemplate') }} as wlt
{%- if is_incremental() %}
    where wlt.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where wlt.[IsDelete] is null
{% endif %}