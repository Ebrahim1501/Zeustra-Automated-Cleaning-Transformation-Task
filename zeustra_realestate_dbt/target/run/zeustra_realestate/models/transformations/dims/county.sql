
  
    

  create  table "Zeustra"."public"."county__dbt_tmp"
  
  
    as
  
  (
    

with all_county
AS
(


 SELECT DISTINCT 
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(dbusa.county_description)),'county',''),'city',''),'parish',''))



    
 AS name,st.id AS state
 FROM "Zeustra"."public"."dbusa_tenants" dbusa JOIN "Zeustra"."public"."state" st ON 
    LOWER(TRIM(dbusa.state))
=st.abbrivation 
 WHERE dbusa.county_description IS NOT NULL 

UNION

SELECT DISTINCT 
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(reonomy.county)),'county',''),'city',''),'parish',''))



    
 AS name,st.id AS state
FROM "Zeustra"."public"."reonomy_properties" reonomy JOIN "Zeustra"."public"."state" st ON 
    LOWER(TRIM(reonomy.state))
=st.abbrivation 
WHERE reonomy.county IS NOT NULL 



)

select md5(concat(name,state::text)) as id,* from all_county
  );
  