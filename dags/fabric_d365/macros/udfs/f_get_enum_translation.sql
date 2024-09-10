{% macro create_f_get_enum_translation() %}
GO
create or alter function stage.f_get_enum_translation (@tablename varchar(200), @langcode varchar(10) = '1033')
returns table
as
return (
    select concat('{', string_agg(convert(nvarchar(max), idvalue), ','), '}') as enumtranslation
    from
        (
            select
                concat(
                    '"', optionsetname, '":{'
                    , string_agg(convert(nvarchar(max), concat('"', [option], '":"', localizedlabel, '"')), ',')
                    , '}'
                ) as idvalue
            from {{ ref('vw_d365_globaloptionsetmetadata') }}
            where entityname = @tablename
                and localizedlabellanguagecode = @langcode
            group by optionsetname
        ) as x
);
GO
{% endmacro %}
