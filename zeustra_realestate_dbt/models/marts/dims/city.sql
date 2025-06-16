
{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}


with allcities AS
(



SELECT  DISTINCT {{dim_name("dbusa.city")}} AS name,county.id as county,state.id as state 
FROM{{ref('dbusa_tenants')}} as dbusa 
JOIN {{ref('state')}}as state
    ON state.abbrivation=LOWER(dbusa.state) 
 JOIN {{ref('county')}} as county
    ON county.name={{ standardize_county('dbusa.county_description') }} AND county.state=state.id
WHERE dbusa.city IS NOT NULL

UNION

SELECT DISTINCT {{dim_name("reonomy.city")}} AS name,county.id as county,state.id as state FROM
{{ref('reonomy_properties')}} as reonomy JOIN {{ref('state')}} as state
    ON LOWER(TRIM(reonomy.state))=state.abbrivation
JOIN {{ref('county')}} as county ON county.name={{ standardize_county('reonomy.county') }} AND county.state=state.id
WHERE reonomy.city IS NOT NULL


) 

SELECT md5(concat(name,county::text)) as id,* FROM allcities --use fk value of county & city name as pk  