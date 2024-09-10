{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_inventserial_sk']
) }}

select
    sr.[Id] as dim_d365_inventserial_sk
    , sr.recid as inventserial_recid
    , sr.itemid
    , i.speciescode
    , i.processstatecode
    , sr.dxc_netweight as netweight
    , sr.dxc_grossweight
        as grossweight
    , sr.dxc_haultrawlid as haultrawlid
    , sr.dxc_prodshift
        as prodshift
    , sr.partition
    /* custom Antarctic Toothfish categories against the SR \ whole fish being caught */
    , sr.rfidtagid
    , sr.[IsDelete]
    , sr.versionnumber
    , sr.sysrowversion
    /* --count of 'pieces of fish' in a 'box' at the SN# level, Used in the detailed Transport Loadout Report and so will be needed for SC reports */
    , upper(sr.inventserialid) as inventserialid
    , case when convert(date, sr.proddate) = '1900-01-01' then null else convert(date, sr.proddate) end as proddate
    , upper(sr.dataareaid) as inventserial_dataareaid
    , case when i.speciescode in ('ATO', 'PTO')
            then
                case
                    when sr.dxc_netweight >= 00 and sr.dxc_netweight < 02 then 'TOA 00-02 kg'
                    when sr.dxc_netweight >= 02 and sr.dxc_netweight < 03 then 'TOA 02-03 kg'
                    when sr.dxc_netweight >= 03 and sr.dxc_netweight < 04 then 'TOA 03-04 kg'
                    when sr.dxc_netweight >= 04 and sr.dxc_netweight < 05 then 'TOA 04-05 kg'
                    when sr.dxc_netweight >= 05 and sr.dxc_netweight < 06 then 'TOA 05-06 kg'
                    when sr.dxc_netweight >= 06 and sr.dxc_netweight < 08 then 'TOA 06-08 kg'
                    when sr.dxc_netweight >= 08 and sr.dxc_netweight < 10 then 'TOA 08-10 kg'
                    when sr.dxc_netweight >= 10 and sr.dxc_netweight < 12 then 'TOA 10-12 kg'
                    when sr.dxc_netweight >= 12 and sr.dxc_netweight < 15 then 'TOA 12-15 kg'
                    when sr.dxc_netweight >= 15 and sr.dxc_netweight < 20 then 'TOA 15-20 kg'
                    when sr.dxc_netweight >= 20 and sr.dxc_netweight < 30 then 'TOA 20-30 kg'
                    when sr.dxc_netweight >= 30 and sr.dxc_netweight < 40 then 'TOA 30-40 kg'
                    when sr.dxc_netweight >= 40 and sr.dxc_netweight < 50 then 'TOA 40-50 kg'
                    when sr.dxc_netweight >= 50 and sr.dxc_netweight < 1000 then 'TOA 50+ kg'
                end
    end as serial_size
    , upper(sr.dxc_tripid) as tripid
    , case when i.speciescode in ('ATO', 'PTO') then 1 else sr.dxc_pieces end as pieces
from {{ source('fno', 'inventserial') }} as sr
left join {{ ref('dim_d365_item') }} as i on sr.itemid = i.itemid and upper(sr.dataareaid) = i.item_dataareaid
{%- if is_incremental() %}
    where sr.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where sr.[IsDelete] is null
{% endif %}
