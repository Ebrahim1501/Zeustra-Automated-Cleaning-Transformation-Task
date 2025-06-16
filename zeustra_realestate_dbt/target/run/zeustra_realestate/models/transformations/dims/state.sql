
  
    

  create  table "Zeustra"."public"."state__dbt_tmp"
  
  
    as
  
  (
    


with all_abbrivations AS
(
SELECT DISTINCT 
    LOWER(TRIM(state))
 as abbrivation
FROM "Zeustra"."public"."reonomy_properties"
UNION
SELECT DISTINCT 
    LOWER(TRIM(state))
 as abbrivation
FROM "Zeustra"."public"."dbusa_tenants"
),
states_names AS --must run dbt seed before to get the states lookup table
(

    SELECT lookup.name,abrv.abbrivation
    FROM all_abbrivations abrv LEFT JOIN "Zeustra"."public"."states_lookup" lookup
    ON abrv.abbrivation=
    LOWER(TRIM(lookup.standard_abbrviation))

	OR 
	abrv.abbrivation=
    LOWER(TRIM(lookup.informal_abbrviation))
-- in case the source used an informal state abbriviation 


)
select MD5(abbrivation)as id, * from states_names
  );
  