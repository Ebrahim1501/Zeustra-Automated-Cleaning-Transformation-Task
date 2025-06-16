
  
    

  create  table "Zeustra"."public"."address__dbt_tmp"
  
  
    as
  
  (
    

WITH all_addresses 
AS
(
    SELECT DISTINCT TRIM(lower(dbusa.address)) AS street_address,state.id AS state,county.id AS county,city.id AS city,zipcode.id AS zipcode,state.abbrivation AS state_name,city.name AS city_name,zipcode.zip_code AS zip_code
    FROM"Zeustra"."public"."dbusa_tenants" AS dbusa 
    LEFT JOIN "Zeustra"."public"."state" AS state ON state.abbrivation=
    LOWER(TRIM(dbusa.state))

    LEFT JOIN "Zeustra"."public"."county" AS county ON county.name=
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(dbusa.county_description)),'county',''),'city',''),'parish',''))



    
 AND county.state=state.id
    LEFT JOIN "Zeustra"."public"."city" AS city ON city.name=
    LOWER(TRIM(dbusa.city))
 AND city.state=state.id AND city.county=county.id
    LEFT JOIN "Zeustra"."public"."zipcode" AS zipcode ON zipcode.zip_code=
    REGEXP_REPLACE(dbusa.zip_code, '[^0-9]', '', 'g')
 AND zipcode.state=state.id AND zipcode.county=county.id AND zipcode.city=city.id
    WHERE dbusa.address IS NOT NULL  

UNION

    SELECT DISTINCT TRIM(lower(reonomy.address)) AS street_address,state.id AS state,county.id AS county,city.id AS city,zipcode.id AS zipcode,state.abbrivation AS state_name,city.name AS city_name,zipcode.zip_code AS zip_code
    FROM"Zeustra"."public"."reonomy_properties" AS reonomy 
    LEFT JOIN "Zeustra"."public"."state" AS state ON state.abbrivation=
    LOWER(TRIM(reonomy.state))

    LEFT JOIN "Zeustra"."public"."county" AS county ON county.name=
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(reonomy.county)),'county',''),'city',''),'parish',''))



    
 AND county.state=state.id
    LEFT JOIN "Zeustra"."public"."city" AS city ON city.name=
    LOWER(TRIM(reonomy.city))
 AND city.state=state.id AND city.county=county.id
    LEFT JOIN "Zeustra"."public"."zipcode" AS zipcode ON zipcode.zip_code=
    REGEXP_REPLACE(reonomy.zip, '[^0-9]', '', 'g')
 AND zipcode.state=state.id AND zipcode.county=county.id AND zipcode.city=city.id
    WHERE reonomy.address IS NOT NULL  



),
append_full_address AS
(
    SELECT street_address,state,county,city,zipcode,
    CONCAT(LOWER(TRIM(street_address)),',',LOWER(TRIM(city_name)),',',LOWER(TRIM(state_name)),',',zip_code) AS full_address
FROM all_addresses


),
standarized_address AS
(
    SELECT *,standardize_address
    (
    'tiger.pagc_lex',
    'tiger.pagc_gaz',
    'tiger.pagc_rules',
    full_address
                ) AS standarize_address
    FROM append_full_address


)

select md5(concat(street_address,zipcode,city,county,state)) as id,* from standarized_address
  );
  