{% macro nullify_invalid_date(target_column) -%}
case when {{ target_column }} in ('1900-01-01', '1900-01-01 00:00:00.000', '2154-12-31', '2154-12-31 23:59:59.000') then null else {{ target_column }} end 
{%- endmacro %}