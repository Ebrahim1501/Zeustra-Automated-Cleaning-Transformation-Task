{% macro clean_zipcode(zipcode_column) %}
    REGEXP_REPLACE({{ zipcode_column }}, '[^0-9]', '', 'g')
{% endmacro %}