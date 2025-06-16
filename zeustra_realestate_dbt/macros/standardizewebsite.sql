{% macro clean_website(website_column) %}
    LOWER(
        TRIM(
            SPLIT_PART(

                REGEXP_REPLACE(
                    {{ website_column }},
                    '^(https?:\/\/)?(www\.)?', 
                    ''
                                ),
                '/', 1 
                     )
        )
    )
{% endmacro %}