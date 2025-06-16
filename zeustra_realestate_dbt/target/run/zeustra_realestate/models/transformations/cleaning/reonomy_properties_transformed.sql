
  
    

  create  table "Zeustra"."public"."reonomy_properties_transformed__dbt_tmp"
  
  
    as
  
  (
    


with columns_selected AS
(
SELECT DISTINCT

TRIM(apn) AS apn,

    LOWER(TRIM(state))
 AS state,

     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(county)),'county',''),'city',''),'parish',''))



    
 AS county,

    LOWER(TRIM(city))
 AS city,

    REGEXP_REPLACE(zip, '[^0-9]', '', 'g')
 AS zip_code,
LOWER(TRIM(address)) AS address,
TRIM(block_id) AS block_id,
TRIM(census_tract)::bigint AS census_tract,
TRIM(depth)::Float AS depth,
TRIM(floors)::int AS number_of_floors,
TRIM(number_of_units)::INT AS number_of_units,
TRIM(number_of_buildings)::INT AS number_of_buildings,
TRIM(frontage)::NUMERIC AS frontage,
TRIM(area_acres)::NUMERIC AS land_area_acres,
TRIM(area_sqft)::NUMERIC AS land_area_sqft,
ROUND(TRIM(area_sqft)::NUMERIC,2)AS land_area_sqft_rounded,
(TRIM(floors)::int/TRIM(area_sqft)::FLOAT) AS existing_floor_area_ratio,   
LOWER(TRIM(neighborhood_name)) AS neighborhood_name,
TRIM(longitude)::FLOAT AS longitude,
TRIM(latitude)::FLOAT AS latitude,
legal_description ,
mcd_name,
msa_name,
original_source_id,
LOWER(property_type) AS property_type,
LOWER(property_subtype) AS property_subtype,
owner_address,
TRIM(last_update_time)::date AS last_modified_date,
TRIM(year_built)::INT AS year_built,
TRIM(year_renovated)::INT AS year_renovated,
COALESCE(TRIM(building_area_sqft),TRIM(sum_building_sqft)) ::NUMERIC AS total_building_sqft,
TRIM(residential_units)::INT as residential_units,

sales_info AS sales_jsonb


FROM "Zeustra"."public"."reonomy_properties"

)
,
flatten_sales AS
(
    SELECT original_source_id,jsonb_array_elements(sales_jsonb) AS sales_jsonb FROM columns_selected



),

extract_sales_info AS

(
    SELECT original_source_id,last_sale_date,last_sale_price,last_sale_price_per_acre_lot,last_transaction_type 
    
    FROM
    (
        SELECT 
        original_source_id,
        TRIM(sales_jsonb->>'sale_date')::DATE AS last_sale_date,
        TRIM(sales_jsonb->>'price_per_acre_lot_area')::NUMERIC AS last_sale_price_per_acre_lot,
        TRIM(sales_jsonb->>'sale_amount')::NUMERIC AS last_sale_price,
        TRIM(sales_jsonb->>'transaction_type') AS last_transaction_type,
        ROW_NUMBER() OVER (PARTITION BY original_source_id ORDER BY TRIM(sales_jsonb->>'sale_date')::DATE ) AS transaction_order
        FROM flatten_sales
    ) 
    AS s

    WHERE transaction_order=1



),

combined AS
(
    SELECT  
    p.original_source_id,
    p.apn,
    p.state,
    p.county,
    p.city,
    p.zip_code,
    p.address,
    p.block_id,
    p.census_tract,
    p.depth,
    p.number_of_floors,
    p.number_of_units,
    p.number_of_buildings,
    p.residential_units,
    p.frontage,
    p.land_area_acres,
    p.land_area_sqft,
    p.land_area_sqft_rounded,
    p.total_building_sqft,
    p.existing_floor_area_ratio,
    p.neighborhood_name,
    p.longitude,
    p.latitude,
    p.legal_description,
    p.mcd_name,
    p.msa_name,
    p.property_type,
    p.property_subtype,
    p.owner_address,
    p.last_modified_date,
    p.year_built,
    p.year_renovated, 
    
    s.last_sale_date,
    s.last_sale_price,
    s.last_sale_price_per_acre_lot,
    (s.last_sale_price/p.land_area_sqft) AS last_sale_price_per_sqft,
    ROUND(s.last_sale_price,2) AS last_sale_price_rounded ,
    s.last_transaction_type
     FROM columns_selected p LEFT JOIN extract_sales_info s ON p.original_source_id=s.original_source_id





)




SELECT * from combined
  );
  