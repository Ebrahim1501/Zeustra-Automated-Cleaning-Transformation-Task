{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}

with all_county
AS
(


 SELECT DISTINCT {{ standardize_county('dbusa.county_description') }} AS name,st.id AS state
 FROM {{ref('dbusa_tenants')}} dbusa JOIN {{ref('state')}} st ON {{dim_name("dbusa.state")}}=st.abbrivation 
 WHERE dbusa.county_description IS NOT NULL 

UNION

SELECT DISTINCT {{ standardize_county('reonomy.county') }} AS name,st.id AS state
FROM {{ref('reonomy_properties')}} reonomy JOIN {{ref('state')}} st ON {{dim_name("reonomy.state")}}=st.abbrivation 
WHERE reonomy.county IS NOT NULL 



)

select md5(concat(name,state::text)) as id,* from all_county