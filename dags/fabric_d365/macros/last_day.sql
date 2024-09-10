{% macro last_day(date, period='month') %}
    {% if period == 'month' %}
        {{ return("EOMONTH(" ~ date ~ ")") }}
    {% elif period == 'week' %}
        {{ return("DATEADD(DAY, 7 - DATEPART(WEEKDAY, " ~ date ~ "), " ~ date ~ ")") }}
    {% elif period == 'quarter' %}
        {{ return("
            DATEADD(DAY, -1, 
                DATEADD(QUARTER, DATEDIFF(QUARTER, 0, " ~ date ~ ") + 1, 0)
            )
        ") }}
    {% elif period == 'year' %}
        {{ return("
            DATEADD(DAY, -1, 
                DATEADD(YEAR, DATEDIFF(YEAR, 0, " ~ date ~ ") + 1, 0)
            )
        ") }}
    {% else %}
        {{ exceptions.raise_compiler_error("Unsupported period: " ~ period) }}
    {% endif %}
{% endmacro %}
