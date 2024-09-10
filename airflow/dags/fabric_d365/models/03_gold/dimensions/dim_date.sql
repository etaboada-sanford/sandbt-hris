{{
    config(
        materialized = "table"
    )
}}

{%- set current_timestamp = modules.datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') -%}

with source as (
    select
        cast(convert(varchar, date, 112) as int) as dim_date_key
        , date as calendar_date
        , cast(convert(varchar, date, 112) as int) as month_key
        , cast('{{ current_timestamp }}' as date) as today
        , day(date) as day_num_in_month
        , month(date) as month_num_in_year
        , year(date) as [year]
        , datepart(weekday, date) as day_num_in_week
        , case
            when month(date) = 1 then 'January'
            when month(date) = 2 then 'February'
            when month(date) = 3 then 'March'
            when month(date) = 4 then 'April'
            when month(date) = 5 then 'May'
            when month(date) = 6 then 'June'
            when month(date) = 7 then 'July'
            when month(date) = 8 then 'August'
            when month(date) = 9 then 'September'
            when month(date) = 10 then 'October'
            when month(date) = 11 then 'November'
            when month(date) = 12 then 'December'
        end as month_name
        , case
            when datepart(weekday, date) = 1 then 'Sunday'
            when datepart(weekday, date) = 2 then 'Monday'
            when datepart(weekday, date) = 3 then 'Tuesday'
            when datepart(weekday, date) = 4 then 'Wednesday'
            when datepart(weekday, date) = 5 then 'Thursday'
            when datepart(weekday, date) = 6 then 'Friday'
            when datepart(weekday, date) = 7 then 'Saturday'
        end as day_name
        , eomonth(date) as month_end_date
        , dateadd(day, 1, eomonth(date, -1)) as month_start_date
        , dateadd(day, 1 - datepart(weekday, date), date) as week_start_day
        , dateadd(day, 7 - datepart(weekday, date), date) as week_end_day
        , dateadd(day, 1, eomonth(date, (datepart(quarter, date) - 1) * 3)) as quarter_end_day
        , dateadd(day, 1, eomonth(date, (datepart(quarter, date) - 1) * 3 - 3)) as quarter_start_day
        , datepart(quarter, date) as quarter
        , case when month(date) <= 9 then year(date) else year(date) + 1 end as fiscal_year
        , case when month(date) <= 9 then datepart(quarter, date) + 1 else datepart(quarter, date) - 3 end as fiscal_quarter
    from (
        select cast(dateadd(day, value, '{{ var("dim_date_from") }}') as datetime2(6)) as [date]
        from generate_series(0, datediff(day, '{{ var("dim_date_from") }}', '{{ var("dim_date_to") }}'))
    ) as daterange
)

select dim_date_key
    , calendar_date
    , day_num_in_month
    , month_num_in_year
    , year
    , day_num_in_week
    , month_name
    , day_name
    , month_end_date
    , month_start_date
    , week_start_day
    , week_end_day
    , quarter_end_day
    , quarter_start_day
    , today
    , case when calendar_date = cast('{{ current_timestamp }}' as date) then 1 else 0 end as yesterday
    , case when month(calendar_date) = month('{{ current_timestamp }}') and year(calendar_date) = year('{{ current_timestamp }}') then 1 else 0 end as this_month
    , case when datepart(quarter, calendar_date) = datepart(quarter, '{{ current_timestamp }}') and year(calendar_date) = year('{{ current_timestamp }}') then 1 else 0 end as current_quarter
    , case when datepart(week, calendar_date) = datepart(week, '{{ current_timestamp }}') and year(calendar_date) = year('{{ current_timestamp }}') then 1 else 0 end as current_week
    , case when month(calendar_date) = month(dateadd(month, -1, '{{ current_timestamp }}')) and year(calendar_date) = year(dateadd(month, -1, '{{ current_timestamp }}')) then 1 else 0 end as last_month
    , case when calendar_date between dateadd(month, -3, '{{ current_timestamp }}') and '{{ current_timestamp }}' then 1 else 0 end as previous_3_months
    , case when calendar_date between dateadd(month, -12, '{{ current_timestamp }}') and '{{ current_timestamp }}' then 1 else 0 end as previous_12_months
    , case when calendar_date between dateadd(day, -30, '{{ current_timestamp }}') and '{{ current_timestamp }}' then 1 else 0 end as previous_30_days
    , case when calendar_date between dateadd(day, -60, '{{ current_timestamp }}') and '{{ current_timestamp }}' then 1 else 0 end as previous_60_days
    , case when calendar_date between dateadd(month, -3, '{{ current_timestamp }}') and '{{ current_timestamp }}' or month(calendar_date) = month('{{ current_timestamp }}') and year(calendar_date) = year('{{ current_timestamp }}') then 1 else 0 end as last_3_months
    , case when calendar_date between dateadd(month, -12, '{{ current_timestamp }}') and '{{ current_timestamp }}' or month(calendar_date) = month('{{ current_timestamp }}') and year(calendar_date) = year('{{ current_timestamp }}') then 1 else 0 end as last_12_months
    , case when calendar_date between dateadd(day, -30, '{{ current_timestamp }}') and '{{ current_timestamp }}' or calendar_date = cast('{{ current_timestamp }}' as date) then 1 else 0 end as last_30_days
    , case when calendar_date between dateadd(day, -60, '{{ current_timestamp }}') and '{{ current_timestamp }}' or calendar_date = cast('{{ current_timestamp }}' as date) then 1 else 0 end as last_60_days
    , left(month_name, 3) as month_abbrev
    , 'FY ' + cast(case when month(calendar_date) <= 9 then year(calendar_date) else year(calendar_date) + 1 end as varchar) as fy_year
    , 'Q' + cast(case when month(calendar_date) <= 9 then datepart(quarter, calendar_date) + 1 else datepart(quarter, calendar_date) - 3 end as varchar) as fy_quarter
    , 'Quarter ' + cast(case when month(calendar_date) <= 9 then datepart(quarter, calendar_date) + 1 else datepart(quarter, calendar_date) - 3 end as varchar) as fy_quarters
    , left(day_name, 3) as day_abbrev
    , case
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 1 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Sunday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 2 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Monday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 3 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Tuesday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 4 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Wednesday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 5 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Thursday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 6 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Friday'
        when datepart(weekday, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date)) = 7 then convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' Saturday'
    end as week_start_date_name
    , case
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 1 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Sunday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 2 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Monday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 3 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Tuesday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 4 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Wednesday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 5 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Thursday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 6 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Friday'
        when datepart(weekday, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date)) = 7 then convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) + ' Saturday'
    end as week_end_date_name
    , convert(varchar, dateadd(day, 1 - datepart(weekday, calendar_date), calendar_date), 120) + ' -to- ' + convert(varchar, dateadd(day, 7 - datepart(weekday, calendar_date), calendar_date), 120) as week_st_end
    , 'FY '
    + cast(case when month(calendar_date) <= 9 then year(calendar_date) else year(calendar_date) + 1 end as varchar)
    + ' - Q'
    + cast(case when month(calendar_date) <= 9 then datepart(quarter, calendar_date) + 1 else datepart(quarter, calendar_date) - 3 end as varchar) as fy_year_quarter
    , convert(varchar, calendar_date, 107) as full_date_desc
from source