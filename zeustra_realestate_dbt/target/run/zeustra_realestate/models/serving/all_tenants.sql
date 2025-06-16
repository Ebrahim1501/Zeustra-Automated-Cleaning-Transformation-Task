
      
  
    

  create  table "Zeustra"."public"."all_tenants"
  
  
    as
  
  (
    with dbusa AS
(   SELECT
    id,
    company_name,
    website,
    company_phone,
    alternative_name,
    dbusa_id as source_property_id,
    employee_total,
    year_established,
    naics,
    naics_description,
    sic,
    sic_description,
    sales_volume,
    type_of_location,
    state,
    --county,
    city,
    zip_code,
    street_address,
    'dbusa' AS source_table

    FROM "Zeustra"."public"."dbusa_tenants_transformed"

),


reonomy 
AS
(   SELECT
    id,
    company_name,
    website,
    company_phone,
    company_name_2 AS alternative_name,
    source_property_id,
    employee_total,
    year_started AS year_established,
    naics_code AS naics,
    naics_description,
    sic_code AS sic,
    sic_description,
    sales_volume,
    type_of_location,
    state,
    --county,
    city,
    zip_code,
    street_address,
    'reonomy' AS source_table
    FROM "Zeustra"."public"."reonomy_tenants_transformed"

),

combined AS
(

SELECT * FROM (select * from reonomy UNION select * from dbusa)



),
deduplication AS
(
    SELECT 
    id,
    company_name,
    website,
    company_phone,
     alternative_name,
    source_property_id,
    employee_total,
     year_established,
     naics,
    naics_description,
     sic,
    sic_description,
    sales_volume,
    type_of_location,
    state,
    --county,
    city,
    zip_code,
    street_address,
     source_table
     
     FROM 
     (
        SELECT *, ROW_NUMBER() OVER(PARTITION BY source_property_id,company_name,naics,sic
                                    
                                    ORDER BY 
                                     company_name DESC NULLS LAST,
                                        website DESC NULLS LAST,
                                        company_phone DESC NULLS LAST,
                                        alternative_name DESC NULLS LAST,
                                        source_property_id DESC NULLS LAST,
                                        employee_total DESC NULLS LAST,
                                        year_established DESC NULLS LAST,
                                        naics DESC NULLS LAST,
                                        naics_description DESC NULLS LAST,
                                        sic DESC NULLS LAST,
                                        sic_description DESC NULLS LAST,
                                        sales_volume DESC NULLS LAST,
                                        type_of_location DESC NULLS LAST,
                                        state DESC NULLS LAST,
                                        --county,
                                        city DESC NULLS LAST,
                                        zip_code DESC NULLS LAST,
                                        street_address DESC NULLS LAST
                                    
                                    
                                    
                                    
                                    
                                    ) as row_num
        
        FROM combined


     )

     WHERE row_num=1



)
select * from deduplication
  );
  
  