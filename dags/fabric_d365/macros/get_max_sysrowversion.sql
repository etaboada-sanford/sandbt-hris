{% macro get_max_sysrowversion() %}
{% if execute and is_incremental() %}
{% set query %}
SELECT coalesce(max(sysrowversion), 0) as maxversion FROM {{ this }};
{% endset %}
{% set maxversion = run_query(query).columns[0][0] %}
{% do return(maxversion) %}
{% endif %}
{% endmacro %}