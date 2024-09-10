{{ config(
    materialized = 'incremental', 
    unique_key = ['dim_d365_signinglimitpolicyrule_sk']
) }}

select
    spr.[Id] as dim_d365_signinglimitpolicyrule_sk
    , spr.recid as syspolicyrule_recid
    , sp.recid as syspolicy_recid
    , sp.name as syspolicyname
    , sp.description as syspolicydescription

    , {{ translate_enum('ept', 'spt.policytype' ) }} as policytype
    , {{ translate_enum('ehp', 'spt.hierarchypurpose' ) }} as hierarchypurpose

    , sprt.includeparentrules
    , sprt.name as ruletypename
    , sprt.ruleformname

    , case when /* spr.validfromtzid = 37001 */ 1 = 1 then sprvf.newzealandtime else spr.validfrom end as validfrom_nzt
    , case when /* spr.validtotzid = 37001   */ 1 = 1 then sprvt.newzealandtime else spr.validto end as validto_nzt
    , case
        when
            case when/*  spr.validfromtzid = 37001 */ 1 = 1 then sprvf.newzealandtime else spr.validfrom end <= cast(getdate() as date)
            and
            case when/*  spr.validtotzid = 37001 */ 1 = 1 then sprvt.newzealandtime else spr.validto end > cast(getdate() as date)
            then 1
        else 0
    end as isvalid
    , sp.[IsDelete]
    , sp.versionnumber
    , sp.sysrowversion

from {{ source('fno', 'syspolicy') }} as sp

left join {{ source('fno', 'syspolicytype') }} as spt
    on sp.policytype = spt.recid
        and spt.[IsDelete] is null

left join {{ source('fno', 'syspolicyrule') }} as spr
    on sp.recid = spr.policy
        and spr.[IsDelete] is null

left join {{ source('fno', 'syspolicyruletype') }} as sprt
    on spr.policyruletype = sprt.recid
        and sprt.[IsDelete] is null

cross apply dbo.f_get_enum_translation('syspolicytype', '1033') as ehp
cross apply dbo.f_get_enum_translation('syspolicytype', '1033') as ept
cross apply dbo.f_convert_utc_to_nzt(spr.validfrom) as sprvf
cross apply dbo.f_convert_utc_to_nzt(spr.validto) as sprvt

{%- if is_incremental() %}
    where sp.sysrowversion > {{ get_max_sysrowversion() }}
{%- else %}
    where sp.[IsDelete] is null
{% endif %}
