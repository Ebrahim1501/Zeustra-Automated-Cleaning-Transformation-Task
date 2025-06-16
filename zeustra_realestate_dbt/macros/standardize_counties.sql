{% macro standardize_county(county_column) %}
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM({{county_column}})),'county',''),'city',''),'parish',''))



    
{% endmacro %}
