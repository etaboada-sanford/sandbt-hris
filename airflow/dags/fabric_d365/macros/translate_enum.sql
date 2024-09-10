{% macro translate_enum(alias, field) -%}
{%- set enum = field.split('.')[1] -%}
cast(nullif(json_value({{ alias }}.enumtranslation, CONCAT('$.{{ enum }}."', {{ field }},'"')), '') as varchar(2048))
{%- endmacro %}