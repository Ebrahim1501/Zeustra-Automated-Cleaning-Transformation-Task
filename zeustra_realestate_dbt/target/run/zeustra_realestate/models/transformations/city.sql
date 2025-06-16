
  
    

  create  table "Zeustra"."public"."city__dbt_tmp"
  
  
    as
  
  (
    


with allcities AS
(



SELECT  DISTINCT 
    LOWER(TRIM(dbusa.city))
 AS name,county.id as county,state.id as state 
FROM"Zeustra"."public"."dbusa_tenants" as dbusa 
JOIN "Zeustra"."public"."state"as state
    ON state.abbrivation=LOWER(dbusa.state) 
 JOIN "Zeustra"."public"."county" as county
    ON county.name=
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(dbusa.county_description)),'county',''),'city',''),'parish',''))



    
 AND county.state=state.id
WHERE dbusa.city IS NOT NULL

UNION

SELECT DISTINCT 
    LOWER(TRIM(reonomy.city))
 AS name,county.id as county,state.id as state FROM
"Zeustra"."public"."reonomy_properties" as reonomy JOIN "Zeustra"."public"."state" as state
    ON LOWER(TRIM(reonomy.state))=state.abbrivation
JOIN "Zeustra"."public"."county" as county ON county.name=
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(reonomy.county)),'county',''),'city',''),'parish',''))



    
 AND county.state=state.id
WHERE reonomy.city IS NOT NULL


) 

SELECT md5(concat(name,county::text)) as id,* FROM allcities --use fk value of county & city name as pk
  );
  