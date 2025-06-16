
WITH
property_clusters
AS
(
SELECT
 m.cluster_id,
 a.*
FROM  {{ref('all_properties')}} a 
LEFT JOIN matched_properties m 
ON m.id=a.id 
),

cluster_ranking
AS
(
    
    SELECT *, ROW_NUMBER() OVER( PARTITION BY cluster_id
                                ORDER BY                                
                                state DESC NULLS LAST,
                                county DESC NULLS LAST,
                                city DESC NULLS LAST, 
                                zip_code DESC NULLS LAST,
                                address DESC NULLS LAST,
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
                                
                                ) as cluster_entity_order
    FROM property_clusters
),

cluster_representative
 AS
(
    SELECT 
    id,    
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
    year_renovated
    FROM cluster_ranking
    WHERE cluster_entity_order = 1
),

address_normalization 
AS

(
    SELECT 
    p.id,    
    p.original_source_id,
    --state,
    --county,
    --city,
    --zip_code,
    address,
    CONCAT('Addr','|',md5(concat(p.address,p.zip_code,p.city,p.county,p.state))) AS address_id,
    
    p.longitude,
    p.latitude,
    p.accessor_parcel_number_raw,
    p.block_id,
    p.census_tract,
    p.legal_description,
    p.depth,
    p.existing_floor_area_ratio,
    p.frontage,
    a.full_address,
    p.land_area_acres,
    p.land_area_sqft,
    p.total_building_sqft,
    p.sqft_min,
    p.sqft_max,
    p.land_area_sqft_rounded,
    p.last_modified_date,
    p.last_sale_date,
    p.last_sale_price_per_sqft,
    p.last_sale_price_rounded,
    p.last_transaction_type,
    p.neighborhood_name,
    p.mcd_name,
    p.msa_name,
    p.number_of_buildings,
    p.number_of_floors,
    p.number_of_units,
    p.residential_units,
    p.owner_address,
    p.property_type,
    p.property_subtype,
    p.year_built,
    p.year_renovated

    FROM cluster_representative p LEFT JOIN address a
    ON a.id=CONCAT('Addr','|',md5(concat(p.address,p.zip_code,p.city,p.county,p.state)))

)



SELECT * FROM address_normalization

