{% macro create_udfs() %}

create schema if not exists {{target.schema}};

{{ create_f_convert_utc_to_nzt() }};
{{ create_f_get_enum_translation() }};

{% endmacro %}