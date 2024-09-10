/* Default exchange rates to use in stage fact scripts */

with cte as (
    select
        er.recid as exchangerate_recid
        , pr.fromcurrencycode
        , pr.tocurrencycode
        , ert.name as exchangerate_name
        , ert.description as exchangerate_description
        , vfrom.newzealandtime as validfrom
        , vto.newzealandtime as validto
        , er.partition
        , er.exchangerate * .01 as exchangerate

        , rank() over (
            partition by fromcurrencycode, tocurrencycode, validfrom, validto
            order by er.createddatetime desc, er.recid desc
        ) as rnk

    from {{ source('fno', 'exchangerate') }} as er
    left join {{ source('fno', 'exchangeratecurrencypair') }} as pr on er.exchangeratecurrencypair = pr.recid
    left join {{ source('fno', 'exchangeratetype') }} as ert on pr.exchangeratetype = ert.recid
    cross apply dbo.f_convert_utc_to_nzt(er.validfrom) as vfrom
    cross apply dbo.f_convert_utc_to_nzt(er.validto) as vto
    where
        er.[IsDelete] is null
        and pr.[IsDelete] is null
        and ert.[IsDelete] is null
        and ert.name = 'Default'
)
select *
from cte
where rnk = 1
