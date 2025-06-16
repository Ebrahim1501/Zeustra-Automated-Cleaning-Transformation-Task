
{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}

WITH cte1 AS
(

SELECT DISTINCT original_source_id,
state,county,city,zip_code,address,longitude,latitude,
NULL AS accessor_parcel_number_raw,
NULL AS block_id,
NULL ::BIGINT AS census_tract,
NULL AS legal_description,
NULL :: NUMERIC AS depth,
NULL::NUMERIC AS existing_floor_area_ratio,
NULL::NUMERIC AS frontage,
NULL::NUMERIC AS land_area_acres,
NULL::NUMERIC AS land_area_sqft,
NULL::NUMERIC AS total_building_sqft,
sqft_min,
sqft_max,
NULL::NUMERIC AS land_area_sqft_rounded,
NULL::date AS last_modified_date,
NULL::date AS last_sale_date,
NULL::NUMERIC AS last_sale_price_per_sqft,
NULL::NUMERIC AS last_sale_price_rounded,
NULL AS last_transaction_type,
NULL AS neighborhood_name,
NULL AS mcd_name,
NULL AS msa_name,
NULL::INT AS number_of_buildings,
NULL::INT AS number_of_floors,
NULL::INT AS number_of_units,
NULL::INT AS residential_units,
NULL AS owner_address,
NULL AS property_type,
NULL AS property_subtype,
NULL::INT AS year_built,
NULL::INT AS year_renovated,

'dbusa' AS source_table
FROM {{ref('dbusa_properties_transformed')}}

),

cte2 AS
(

SELECT DISTINCT original_source_id,
state,county,city,zip_code,address,longitude,latitude,
apn AS accessor_parcel_number_raw,
 block_id,
 census_tract,
 legal_description,
 depth,
existing_floor_area_ratio,
frontage,
land_area_acres,
land_area_sqft,
total_building_sqft,
NULL::NUMERIC AS sqft_min,
NULL::NUMERIC AS sqft_max,
land_area_sqft_rounded,
 last_modified_date,
 last_sale_date,
 last_sale_price_per_sqft,
 last_sale_price_rounded,
 last_transaction_type,
neighborhood_name,
 mcd_name,
 msa_name,
 number_of_buildings,
 number_of_floors,
 number_of_units,
 residential_units,
 owner_address,
 property_type,
 property_subtype,
 year_built,
 year_renovated,

 'reonomy' AS source_table
FROM {{ref('reonomy_properties_transformed')}}





),
deduplication AS
(
SELECT 

original_source_id,
state,
county,
city,
zip_code,
address,
longitude,
latitude,
accessor_parcel_number_raw,
block_id,
census_tract,
legal_description,
depth,
existing_floor_area_ratio,
frontage,
land_area_acres,
land_area_sqft,
total_building_sqft,
sqft_min,
sqft_max,
land_area_sqft_rounded,
last_modified_date,
last_sale_date,
last_sale_price_per_sqft,
last_sale_price_rounded,
last_transaction_type,
neighborhood_name,
mcd_name,
msa_name,
number_of_buildings,
number_of_floors,
number_of_units,
residential_units,
owner_address,
property_type,
property_subtype,
 year_built,
 year_renovated,

source_table
    
FROM
(
            select *,

            ROW_NUMBER() OVER(
                PARTITION BY 
                    state,county,city,zip_code,address
                ORDER BY 
            longitude DESC NULLS LAST,
            latitude DESC NULLS LAST,
            accessor_parcel_number_raw DESC NULLS LAST,
            block_id DESC NULLS LAST,
            census_tract DESC NULLS LAST,
            legal_description DESC NULLS LAST,
            depth DESC NULLS LAST,
            existing_floor_area_ratio DESC NULLS LAST,
            frontage DESC NULLS LAST,
            land_area_acres DESC NULLS LAST,
            land_area_sqft DESC NULLS LAST,
            total_building_sqft DESC NULLS LAST,
            sqft_min DESC NULLS LAST,
            sqft_max DESC NULLS LAST,
            land_area_sqft_rounded DESC NULLS LAST,
            last_modified_date DESC NULLS LAST,
            last_sale_date DESC NULLS LAST,
            last_sale_price_per_sqft DESC NULLS LAST,
            last_sale_price_rounded DESC NULLS LAST,
            last_transaction_type DESC NULLS LAST,
            neighborhood_name DESC NULLS LAST,
            mcd_name DESC NULLS LAST,
            msa_name DESC NULLS LAST,
            number_of_buildings DESC NULLS LAST,
            number_of_floors DESC NULLS LAST,
            number_of_units DESC NULLS LAST,
            residential_units DESC NULLS LAST,
            owner_address DESC NULLS LAST,
            property_type DESC NULLS LAST,
            property_subtype DESC NULLS LAST,
            year_built DESC NULLS LAST,
            year_renovated DESC NULLS LAST
            
                      ) as row_num FROM (SELECT * FROM cte1 UNION SELECT * FROM cte2)
) as q

WHERE row_num =1



)












SELECT CONCAT('property','|',MD5(CONCAT(coalesce(state,''),',',coalesce(city,''),coalesce(county,''),coalesce(zip_code,''),COALESCE(address,'')))) as id,* FROM deduplication

