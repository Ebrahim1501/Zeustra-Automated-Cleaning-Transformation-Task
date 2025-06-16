with columns_selection AS
(
SELECT DISTINCT
property_id AS source_property_id,
TRIM(LOWER(company_name)) AS company_name,
TRIM(LOWER(company_name_2)) AS company_name_2,
CASE WHEN LOWER(TRIM(year_started))='n/a' THEN NULL ELSE TRIM(year_started)::int END AS year_started,
LOWER(TRIM(website)) AS website,
CASE WHEN TRIM(LOWER(employee_total))='n/a' OR TRIM(LOWER(employee_total))::int < 0 THEN NULL ELSE  TRIM(employee_total) END AS employee_total,
LOWER(TRIM(type_of_location)) AS type_of_location,
TRIM(sic_code)::int AS sic_code,
LOWER(TRIM(REPLACE(REPLACE(sic_description,',',' '),'.',' '))) AS sic_description,
TRIM(naics_code)::int AS naics_code,
LOWER(TRIM(REPLACE(REPLACE(naics_description,',',' '),'.',' '))) AS naics_description,
REGEXP_REPLACE(TRIM(company_phone), '[^0-9]', '', 'g') AS company_phone,

    LOWER(TRIM(state))
 AS state,

    LOWER(TRIM(city))
 AS city,

    REGEXP_REPLACE(zip_code, '[^0-9]', '', 'g')
 AS zip_code,
TRIM(street_address) AS street_address,
TRIM(sales_volume)::FLOAT AS sales_volume

FROM "Zeustra"."public"."reonomy_tenants"

),


 dedupl AS
(
    SELECT source_property_id,company_name,company_name_2,year_started,website,employee_total,type_of_location,sic_code,sic_description,naics_code,naics_description,company_phone,state,city,zip_code,street_address,sales_volume
    FROM(
        SELECT *,ROW_NUMBER() 
        OVER (PARTITION BY COALESCE(company_name,company_name_2),city,zip_code 
            ORDER BY employee_total DESC NULLS LAST,year_started,sales_volume DESC NULLS LAST,sic_description DESC NULLS LAST,naics_description DESC NULLS LAST) AS occr
        FROM columns_selection        
        ) AS subq
        WHERE occr=1




)
SELECT md5(concat(COALESCE(company_name,company_name_2),city,zip_code))AS id,* FROM dedupl