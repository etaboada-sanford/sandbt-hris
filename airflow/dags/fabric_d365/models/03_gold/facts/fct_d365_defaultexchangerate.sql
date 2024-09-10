{{ config(
    materialized = 'incremental', 
    unique_key = ['exchangerate_recid']
) }}

/* Default exchange rates to use in stage fact scripts */

with exch as (
    select
        er.recid as exchangerate_recid
        , pr.fromcurrencycode
        , pr.tocurrencycode
        , ert.name as exchangerate_name
        , ert.description as exchangerate_description
        , er.partition
        , er.exchangerate * .01 as exchangerate
        , convert(date, er.validfrom) as validfrom
        , convert(date, er.validto) as validto

        , rank() over (
            partition by fromcurrencycode, tocurrencycode, validfrom, validto
            order by er.createddatetime desc, er.recid desc
        ) as rnk
        , er.versionnumber
        , er.sysrowversion
    from {{ source('fno', 'exchangerate') }} as er
    left join {{ source('fno', 'exchangeratecurrencypair') }} as pr on er.exchangeratecurrencypair = pr.recid
    left join {{ source('fno', 'exchangeratetype') }} as ert on pr.exchangeratetype = ert.recid
    where
        er.[IsDelete] is null
        and pr.[IsDelete] is null
        and ert.[IsDelete] is null
        and ert.name = 'Default'
)

select * from exch where rnk = 1