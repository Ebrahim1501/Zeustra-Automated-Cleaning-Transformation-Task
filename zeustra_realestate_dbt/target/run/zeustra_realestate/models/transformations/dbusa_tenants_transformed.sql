
  
    

  create  table "Zeustra"."public"."dbusa_tenants_transformed__dbt_tmp"
  
  
    as
  
  (
    WITH
selected_columns
AS(
SELECT 
LOWER(TRIM(company_name)) AS company_name,
LOWER(public_description) AS public_description,
LOWER(TRIM(url)) AS website,
REGEXP_REPLACE(TRIM(phone), '[^0-9]', '', 'g') AS company_phone,
LOWER(TRIM(company_name_2)) AS alternative_name, 
dbusa_id AS dbusa_id,
property_id AS source_property_id,
--LOWER(TRIM(years_in_business_range)) AS years_in_business_range,
CASE WHEN LOWER(TRIM(employees_total))='n/a' THEN NULL ELSE TRIM(employees_total)::INT END AS employee_total,
CASE WHEN LOWER(TRIM(year_established)) ='n/a' THEN NULL ELSE TRIM(year_established)::INT END  AS year_established,
TRIM(naics01)::int AS naics01,
LOWER(TRIM(REPLACE(REPLACE(naics01_description,',',' '),'.',' '))) as naics01_description,
TRIM(naics02)::int AS naics02,
LOWER(TRIM(REPLACE(REPLACE(naics02_description,',',' '),'.',' '))) as naics02_description,
TRIM(naics03)::int AS naics03,
LOWER(TRIM(REPLACE(REPLACE(naics03_description,',',' '),'.',' '))) as naics03_description,
TRIM(naics04)::int AS naics04,
LOWER(TRIM(REPLACE(REPLACE(naics04_description,',',' '),'.',' '))) as naics04_description,
TRIM(naics05)::int AS naics05,
LOWER(TRIM(REPLACE(REPLACE(naics05_description,',',' '),'.',' '))) as naics05_description,
TRIM(naics06)::int AS naics06,
LOWER(TRIM(REPLACE(REPLACE(naics06_description,',',' '),'.',' '))) as naics06_description,
TRIM(naics07)::int AS naics07,
LOWER(TRIM(REPLACE(REPLACE(naics07_description,',',' '),'.',' '))) as naics07_description,
TRIM(naics08)::int AS naics08,
LOWER(TRIM(REPLACE(REPLACE(naics08_description,',',' '),'.',' '))) as naics08_description,
TRIM(naics09)::int AS naics09,
LOWER(TRIM(REPLACE(REPLACE(naics09_description,',',' '),'.',' '))) as naics09_description,
TRIM(naics10)::int AS naics10,
LOWER(TRIM(REPLACE(REPLACE(naics10_description,',',' '),'.',' '))) as naics10_description,
TRIM(sic01)::int AS sic01,
LOWER(TRIM(REPLACE(REPLACE(sic01_description,',',' '),'.',' '))) as sic01_description,
TRIM(sic02)::int AS sic02,
LOWER(TRIM(REPLACE(REPLACE(sic02_description,',',' '),'.',' '))) as sic02_description,
TRIM(sic03)::int AS sic03,
LOWER(TRIM(REPLACE(REPLACE(sic03_description,',',' '),'.',' '))) as sic03_description,
TRIM(sic04)::int AS sic04,
LOWER(TRIM(REPLACE(REPLACE(sic04_description,',',' '),'.',' '))) as sic04_description,
TRIM(sic05)::int AS sic05,
LOWER(TRIM(REPLACE(REPLACE(sic05_description,',',' '),'.',' '))) as sic05_description,
TRIM(sic06)::int AS sic06,
LOWER(TRIM(REPLACE(REPLACE(sic06_description,',',' '),'.',' '))) as sic06_description,
TRIM(sic07)::int AS sic07,
LOWER(TRIM(REPLACE(REPLACE(sic07_description,',',' '),'.',' '))) as sic07_description,
TRIM(sic08)::int AS sic08,
LOWER(TRIM(REPLACE(REPLACE(sic08_description,',',' '),'.',' '))) as sic08_description,
TRIM(sic09)::int AS sic09,
LOWER(TRIM(REPLACE(REPLACE(sic09_description,',',' '),'.',' '))) as sic09_description,
TRIM(sic10)::int AS sic10,
LOWER(TRIM(REPLACE(REPLACE(sic10_description,',',' '),'.',' '))) as sic10_description,


sales_volume::FLOAT AS sales_volume,
CASE  
  WHEN LOWER(TRIM(location_level_description)) = 'headquarters' THEN 'hq' 
  WHEN LOWER(TRIM(location_level_description)) = 'subsidiary' THEN 'branch' 
  ELSE LOWER(TRIM(location_level_description)) 
END AS type_of_location,

    LOWER(TRIM(state))
 AS state,

     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(county_description)),'county',''),'city',''),'parish',''))



    
 AS county,

    LOWER(TRIM(city))
 AS city,

    REGEXP_REPLACE(zip_code, '[^0-9]', '', 'g')
 as zip_code,
address AS street_address

FROM "Zeustra"."public"."dbusa_tenants"


 WHERE
 (company_name IS NOT NULL OR company_name_2 IS NOT NULL)

--    (naics01_description IS NOT NULL OR naics02_description IS NOT NULL OR naics03_description IS NOT NULL OR naics04_description IS NOT NULL OR naics05_description IS NOT NULL OR naics06_description IS NOT NULL OR naics07_description IS NOT NULL OR naics08_description IS NOT NULL OR naics09_description IS NOT NULL OR naics10_description IS NOT NULL  )
),


deduplication AS
(
    SELECT * FROM

(
    SELECT *,ROW_NUMBER() 
    OVER(PARTITION BY COALESCE (company_name,alternative_name),state,city,zip_code  ORDER BY sales_volume DESC NULLS LAST,employee_total DESC NULLS LAST,year_established DESC NULLS LAST) as occurance
    FROM selected_columns
)  as  subq

WHERE  occurance =1


),


flattened_naics AS
 (

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics01 AS naics,
naics01_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics01 IS NOT NULL OR naics01_description IS NOT NULL


UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics02 AS naics,
naics02_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics02 IS NOT NULL OR naics02_description IS NOT NULL

UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics03 AS naics,
naics03_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics03 IS NOT NULL OR naics03_description IS NOT NULL


UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics04 AS naics,
naics04_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics04 IS NOT NULL OR naics04_description IS NOT NULL



UNION DISTINCT

SELECT 
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics05 AS naics,
naics05_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics05 IS NOT NULL OR naics05_description IS NOT NULL


UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics06 AS naics,
naics06_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics06 IS NOT NULL OR naics06_description IS NOT NULL



UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics07 AS naics,
naics07_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics07 IS NOT NULL OR naics07_description IS NOT NULL



UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics08 AS naics,
naics08_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics08 IS NOT NULL OR naics08_description IS NOT NULL


UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics09 AS naics,
naics09_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics09 IS NOT NULL OR naics09_description IS NOT NULL



UNION 

SELECT DISTINCT
company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
naics10 AS naics,
naics10_description as naics_description,
sic01,sic01_description,sic02,sic02_description,sic03,sic03_description,sic04,sic04_description,sic05,sic05_description,sic06,sic06_description,sic07,sic07_description,sic08,sic08_description,sic09,sic09_description,sic10,sic10_description,sales_volume,type_of_location,
state,county,city,zip_code,street_address
FROM deduplication
WHERE naics10 IS NOT NULL OR naics10_description IS NOT NULL







),


flattened_sics
AS
(
SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic01 AS sic,
 sic01_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic01 IS NOT NULL OR sic01_description IS NOT NULL


UNION


SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic02 AS sic,
 sic02_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic02 IS NOT NULL OR sic02_description IS NOT NULL


UNION


SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic03 AS sic,
 sic03_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic03 IS NOT NULL OR sic03_description IS NOT NULL


UNION


SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic04 AS sic,
 sic04_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic04 IS NOT NULL OR sic04_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic05 AS sic,
 sic05_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic05 IS NOT NULL OR sic05_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic06 AS sic,
 sic06_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic06 IS NOT NULL OR sic06_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic07 AS sic,
 sic07_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic07 IS NOT NULL OR sic07_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic08 AS sic,
 sic08_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic08 IS NOT NULL OR sic08_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic09 AS sic,
 sic09_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic09 IS NOT NULL OR sic09_description IS NOT NULL


UNION

SELECT DISTINCT
 company_name,public_description,website,company_phone,alternative_name,dbusa_id,source_property_id,employee_total,year_established,
 naics,naics_description,
 sic10 AS sic,
 sic10_description AS sic_description,
 sales_volume,type_of_location,state,county,city,zip_code,street_address
FROM flattened_naics
WHERE sic10 IS NOT NULL OR sic10_description IS NOT NULL












)


 

SELECT md5(concat(dbusa_id,naics,sic)) as id ,* FROM flattened_sics






---ALL COLUMNS SELECTED FOR COPY&PASTE PURPOSES
-- company_name,
-- public_description,
-- website,
-- company_phone,
-- alternative_name,
-- dbusa_id,
-- source_property_id,
-- employee_total,
-- year_established,
-- naics01,
-- naics01_description,
-- naics02,
-- naics02_description,
-- naics03,
-- naics03_description,
-- naics04,
-- naics04_description,
-- naics05,
-- naics05_description,
-- naics06,
-- naics06_description,
-- naics07,
-- naics07_description,
-- naics08,
-- naics08_description,
-- naics09,
-- naics09_description,
-- naics10,
-- naics10_description,
-- sic01,
-- sic01_description,
-- sic02,
-- sic02_description,
-- sic03,
-- sic03_description,
-- sic04,
-- sic04_description,
-- sic05,
-- sic05_description,
-- sic06,
-- sic06_description,
-- sic07,
-- sic07_description,
-- sic08,
-- sic08_description,
-- sic09,
-- sic09_description,
-- sic10,
-- sic10_description,
-- sales_volume,
-- type_of_location,
-- state,
-- county,
-- city,
-- zip_code,
-- street_address
  );
  