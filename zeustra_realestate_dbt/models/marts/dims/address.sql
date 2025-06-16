{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}

WITH all_addresses 
AS
(
    SELECT DISTINCT TRIM(lower(dbusa.address)) AS street_address,state.id AS state,county.id AS county,city.id AS city,zipcode.id AS zipcode,state.abbrivation AS state_name,county.name AS county_name,city.name AS city_name,zipcode.zip_code AS zip_code
    FROM{{ref('dbusa_tenants')}} AS dbusa 
    LEFT JOIN {{ref('state')}} AS state ON state.abbrivation={{dim_name('dbusa.state')}}
    LEFT JOIN {{ref('county')}} AS county ON county.name={{standardize_county('dbusa.county_description')}} AND county.state=state.id
    LEFT JOIN {{ref('city')}} AS city ON city.name={{dim_name('dbusa.city')}} AND city.state=state.id AND city.county=county.id
    LEFT JOIN {{ref('zipcode')}} AS zipcode ON zipcode.zip_code={{clean_zipcode('dbusa.zip_code')}} AND zipcode.state=state.id AND zipcode.county=county.id AND zipcode.city=city.id
    WHERE dbusa.address IS NOT NULL  


UNION

    SELECT DISTINCT TRIM(lower(reonomy.address)) AS street_address,state.id AS state,county.id AS county,city.id AS city,zipcode.id AS zipcode,state.abbrivation AS state_name,county.name AS county_name,city.name AS city_name,zipcode.zip_code AS zip_code
    FROM{{ref('reonomy_properties')}} AS reonomy 
    LEFT JOIN {{ref('state')}} AS state ON state.abbrivation={{dim_name('reonomy.state')}}
    LEFT JOIN {{ref('county')}} AS county ON county.name={{standardize_county('reonomy.county')}} AND county.state=state.id
    LEFT JOIN {{ref('city')}} AS city ON city.name={{dim_name('reonomy.city')}} AND city.state=state.id AND city.county=county.id
    LEFT JOIN {{ref('zipcode')}} AS zipcode ON zipcode.zip_code={{clean_zipcode('reonomy.zip')}} AND zipcode.state=state.id AND zipcode.county=county.id AND zipcode.city=city.id
    WHERE reonomy.address IS NOT NULL  




),
append_full_address AS
(
    SELECT street_address,state,county,city,zipcode,state_name,county_name,city_name,zip_code,
    CONCAT(LOWER(TRIM(street_address)),',',LOWER(TRIM(city_name)),',',LOWER(TRIM(state_name)),',',zip_code) AS full_address
FROM all_addresses
 where state is not null or city is not null or zipcode is not null 


),
standarized_address AS
(
    SELECT *,standardize_address
    (
    'tiger.pagc_lex',
    'tiger.pagc_gaz',
    'tiger.pagc_rules',
    full_address
                ) AS standardize_address
    FROM append_full_address


)

select CONCAT('Addr','|',md5(concat(street_address,zip_code,city_name,county_name,state_name))) as id,street_address,state,state_name,county,county_name,city,city_name,zipcode,zip_code,full_address,standardize_address from standarized_address

