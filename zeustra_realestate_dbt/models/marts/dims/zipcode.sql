{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}



with allzip_codes AS

( SELECT 
 {{clean_zipcode("dbusa.zip_code")}} as zip_code,city.id AS city,state.id AS state,county.id AS county
 FROM {{ref('dbusa_tenants')}} as dbusa 
 JOIN {{ref('state')}} AS state ON state.abbrivation=LOWER(TRIM(dbusa.state)) 
 JOIN {{ref('county')}} AS county ON TRIM(LOWER(dbusa.county_description)) =county.name AND state.id=county.state
 JOIN{{ref('city')}} AS city ON city.county=county.id AND city.state=state.id AND city.name=LOWER(TRIM(dbusa.city))
WHERE dbusa.zip_code IS NOT NULL
UNION 

SELECT 
{{clean_zipcode("reonomy.zip")}} as zip_code,city.id AS city,state.id AS state,county.id AS county
 FROM {{ref('reonomy_properties')}} as reonomy 
 JOIN {{ref('state')}} AS state ON state.abbrivation=LOWER(TRIM(reonomy.state)) 
 JOIN {{ref('county')}} AS county ON LOWER(TRIM(split_part(TRIM(reonomy.county),' ',1))) = county.name AND state.id=county.state
 JOIN{{ref('city')}} AS city ON city.county=county.id AND city.state=state.id AND city.name=LOWER(TRIM(reonomy.city))
WHERE reonomy.zip IS NOT NULL


)
SELECT md5(concat(zip_code,city)) AS id,* from allzip_codes