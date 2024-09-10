{% test column_existence(model, columns) %}
    select
        column_name
    from
        information_schema.columns
    where
        table_name = '{{ model }}'
        and column_name in ({{ columns | join(', ') }})
    having
        count(column_name) < {{ columns | length }}
{% endtest %}