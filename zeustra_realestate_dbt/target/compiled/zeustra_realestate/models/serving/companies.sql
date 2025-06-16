WITH
tenants_clusters
AS
(
SELECT
 m.cluster_id,
 a.*
FROM  "Zeustra"."public"."all_tenants" a 
LEFT JOIN matched_tenants m 
ON m.id=a.id 
),

cluster_ranking
AS
(
    
    SELECT *, ROW_NUMBER() OVER( PARTITION BY cluster_id
                                ORDER BY                                
                                company_name DESC NULLS LAST,
                                website DESC NULLS LAST,
                                company_phone DESC NULLS LAST, 
                                alternative_name DESC NULLS LAST,
                                source_property_id DESC NULLS LAST,
                                employee_total DESC NULLS LAST,
                                year_established DESC NULLS LAST,
                                naics DESC NULLS LAST,
                                naics_description DESC NULLS LAST,
                                sic DESC NULLS LAST,
                                sic_description DESC NULLS LAST,
                                sales_volume DESC NULLS LAST,
                                type_of_location DESC NULLS LAST,
                                state DESC NULLS LAST,
                                city DESC NULLS LAST,
                                zip_code DESC NULLS LAST,
                                street_address DESC NULLS LAST
                                
                                
                                ) as cluster_entity_order
    FROM tenants_clusters
),


cluster_representative
 AS
(
    SELECT 
    id,
    company_name,
    website,
    company_phone,
    alternative_name,
    source_property_id,
    employee_total,
    year_established,
    naics,
    naics_description,
    sic,
    sic_description,
    sales_volume,
    type_of_location,
    state,
    city,
    zip_code,
    street_address,
    CONCAT_WS(', ', street_address, city) || CONCAT_WS(' ', ', ' || state, zip_code) AS full_address


    FROM cluster_ranking
    WHERE cluster_entity_order = 1
),
add_standardized_address
AS
(SELECT * ,
standardize_address(
            'tiger.pagc_lex',
            'tiger.pagc_gaz',
            'tiger.pagc_rules',
            full_address) AS standardized_address

FROM cluster_representative


),
extract_parts AS
(
    SELECT *,
    'USA' AS country,

     (standardized_address).building AS building,
     (standardized_address).house_num AS house_num,
     (standardized_address).predir AS predir,
     (standardized_address).qual AS qual,
     (standardized_address).pretype AS pretype,
     (standardized_address).name AS street_name,
     (standardized_address).suftype AS suftype,
     (standardized_address).sufdir AS sufdir,
     (standardized_address).unit AS unit,
     (standardized_address).box AS "box",
     (standardized_address).ruralroute AS ruralroute
     
    
    FROM add_standardized_address

)
select * from extract_parts