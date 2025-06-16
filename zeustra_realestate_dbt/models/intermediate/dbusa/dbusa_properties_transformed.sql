{{config(post_hook="ALTER TABLE {{this}} add primary key(original_source_id)")}}


WITH columns_selection AS (
    SELECT DISTINCT
        dbusa_id as original_source_id,
        COALESCE({{dim_name('state')}}, {{dim_name('physical_state')}}) AS state,
        {{standardize_county('county_description')}} AS county,
        COALESCE({{dim_name('city')}}, {{dim_name('physical_city')}}) AS city,
        COALESCE({{clean_zipcode('zip_code')}}, {{clean_zipcode('physical_zip')}}) AS zip_code,
        LOWER(COALESCE(TRIM(address), TRIM(physical_address))) AS address,
        longitude,
        latitude,
CASE
    WHEN square_footage_description ~ '^[0-9,]+ to [0-9,]+$'
        THEN CAST(REPLACE(SPLIT_PART(square_footage_description, ' to ', 1), ',', '') AS FLOAT)
    WHEN square_footage_description LIKE 'Up to %'
        THEN 0.0
    WHEN square_footage_description LIKE '% or more'
        THEN CAST(REPLACE(SPLIT_PART(square_footage_description, ' or more', 1), ',', '') AS FLOAT)
    ELSE NULL
END AS sqft_min,

CASE
    WHEN square_footage_description ~ '^[0-9,]+ to [0-9,]+$'
        THEN CAST(REPLACE(SPLIT_PART(square_footage_description, ' to ', 2), ',', '') AS FLOAT)
    WHEN square_footage_description LIKE 'Up to %'
        THEN CAST(REGEXP_REPLACE(square_footage_description, '[^0-9]', '', 'g') AS FLOAT)
    WHEN square_footage_description LIKE '% or more'
        THEN 'infinity'::FLOAT
    ELSE NULL
END AS sqft_max
FROM {{ref('dbusa_tenants')}}
),

 deduplication AS

(
    SELECT DISTINCT original_source_id,state,county,city,zip_code,address,longitude,latitude,sqft_min,sqft_max

    FROM(
        SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY state,county,city,zip_code,address,longitude,latitude ORDER BY sqft_max DESC NULLS LAST,sqft_min DESC NULLS LAST,longitude DESC NULLS LAST,latitude DESC NULLS LAST ) as occurances
        FROM columns_selection
        ) as subquery
        WHERE occurances=1




)

SELECT * FROM deduplication
