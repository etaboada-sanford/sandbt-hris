{% macro create_f_generate_dates() %}
GO
    create or alter function dbo.f_generate_dates (@from_years int, @to_years int)
    returns table
    as
    return
    (
        select dateadd(day, number, dateadd(year, @from_years, datefromparts(year(getdate()), 1, 1))) as date
        from master..spt_values
        where type = 'P' and number <= datediff(day, dateadd(year, @from_years, datefromparts(year(getdate()), 1, 1)), dateadd(year, @to_years, datefromparts(year(getdate()), 12, 31)))
    )
GO
{% endmacro %}