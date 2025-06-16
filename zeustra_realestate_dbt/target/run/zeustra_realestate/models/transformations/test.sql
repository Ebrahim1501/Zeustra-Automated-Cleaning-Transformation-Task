
  
    

  create  table "Zeustra"."public"."test__dbt_tmp"
  
  
    as
  
  (
    select 
     TRIM(REPLACE(REPLACE(REPLACE(LOWER(TRIM(county)),'county',''),'city',''),'parish',''))



    
 as standarized_address
FROM "Zeustra"."public"."reonomy_properties"
  );
  