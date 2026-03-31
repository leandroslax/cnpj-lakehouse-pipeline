{% macro parse_yyyymmdd(column_name) %}
    case
        when trim({{ column_name }}) in ('', '0', '00000000') then null
        else strptime(trim({{ column_name }}), '%Y%m%d')
    end
{% endmacro %}
