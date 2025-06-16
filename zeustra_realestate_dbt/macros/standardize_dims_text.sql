{% macro dim_name(column) %}
    LOWER(TRIM({{ column }}))
{% endmacro %}
