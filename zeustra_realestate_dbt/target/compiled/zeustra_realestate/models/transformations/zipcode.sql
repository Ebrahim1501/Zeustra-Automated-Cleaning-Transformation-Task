



with allzip_codes AS

( SELECT 
 
    REGEXP_REPLACE(dbusa.zip_code, '[^0-9]', '', 'g')
 as zip_code,city.id AS city,state.id AS state,county.id AS county
 FROM "Zeustra"."public"."dbusa_tenants" as dbusa 
 JOIN "Zeustra"."public"."state" AS state ON state.abbrivation=LOWER(TRIM(dbusa.state)) 
 JOIN "Zeustra"."public"."county" AS county ON TRIM(LOWER(dbusa.county_description)) =county.name AND state.id=county.state
 JOIN"Zeustra"."public"."city" AS city ON city.county=county.id AND city.state=state.id AND city.name=LOWER(TRIM(dbusa.city))
WHERE dbusa.zip_code IS NOT NULL
UNION 

SELECT 

    REGEXP_REPLACE(reonomy.zip, '[^0-9]', '', 'g')
 as zip_code,city.id AS city,state.id AS state,county.id AS county
 FROM "Zeustra"."public"."reonomy_properties" as reonomy 
 JOIN "Zeustra"."public"."state" AS state ON state.abbrivation=LOWER(TRIM(reonomy.state)) 
 JOIN "Zeustra"."public"."county" AS county ON LOWER(TRIM(split_part(TRIM(reonomy.county),' ',1))) = county.name AND state.id=county.state
 JOIN"Zeustra"."public"."city" AS city ON city.county=county.id AND city.state=state.id AND city.name=LOWER(TRIM(reonomy.city))
WHERE reonomy.zip IS NOT NULL


)
SELECT md5(concat(zip_code,city)) AS id,* from allzip_codes