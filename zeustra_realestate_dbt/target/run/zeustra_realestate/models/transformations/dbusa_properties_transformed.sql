
  
    

  create  table "Zeustra"."public"."dbusa_properties_transformed__dbt_tmp"
  
  
    as
  
  (
    


WITH columns_selection AS (
    SELECT DISTINCT
        dbusa_id as original_source_id,
        COALESCE(
    LOWER(TRIM(state))
, 
    LOWER(TRIM(physical_state))
) AS state,
        
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(county_description)),'county',''),'city',''),'parish',''))



    
 AS county,
        COALESCE(
    LOWER(TRIM(city))
, 
    LOWER(TRIM(physical_city))
) AS city,
        COALESCE(
    REGEXP_REPLACE(zip_code, '[^0-9]', '', 'g')
, 
    REGEXP_REPLACE(physical_zip, '[^0-9]', '', 'g')
) AS zip_code,
        COALESCE(TRIM(address), TRIM(physical_address)) AS address,
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
FROM "Zeustra"."public"."dbusa_tenants"
),
 deduplication AS

(
    SELECT DISTINCT original_source_id,state,county,city,zip_code,address,longitude,latitude,sqft_min,sqft_max

    FROM(
        SELECT * ,
        ROW_NUMBER() OVER(PARTITION BY state,county,city,zip_code,address,sqft_max,sqft_min ORDER BY sqft_max DESC NULLS LAST,sqft_min DESC NULLS LAST,longitude DESC NULLS LAST,latitude DESC NULLS LAST ) as occurances
        FROM columns_selection
        ) as subquery
        WHERE occurances=1




)

SELECT * FROM deduplication
  );
  