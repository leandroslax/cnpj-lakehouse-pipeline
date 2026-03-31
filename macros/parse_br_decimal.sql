{% macro parse_br_decimal(column_name) %}
    cast(replace(trim({{ column_name }}), ',', '.') as double)
{% endmacro %}
