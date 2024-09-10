{% macro create_f_convert_utc_to_nzt() %}
GO
    create or alter function dbo.f_convert_utc_to_nzt (@utcdatetime datetime2(6))
    returns table
    as
    return
    (
        select 
            case 
                when @utcdatetime = '1900-01-01 00:00:00.000' then null
                when @utcdatetime >= dateadd(hour, 2, cast(dateadd(day, (7 - datepart(weekday, datefromparts(year(@utcdatetime), 9, 24))) % 7, datefromparts(year(@utcdatetime), 9, 24)) as datetime2(6)))
                    and @utcdatetime < dateadd(hour, 3, cast(dateadd(day, (7 - datepart(weekday, datefromparts(year(@utcdatetime), 4, 1))) % 7, datefromparts(year(@utcdatetime), 4, 1)) as datetime2(6)))
                then dateadd(hour, 13, @utcdatetime)
                else dateadd(hour, 12, @utcdatetime)
            end as newzealandtime
    )
GO
{% endmacro %}