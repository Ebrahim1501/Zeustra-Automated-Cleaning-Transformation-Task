WITH companies_to_properties
AS
(
    SELECT  DISTINCT c.id AS company_id,p.id AS property_id,
    CASE when c.id LIKE 'dbusa%' THEN 'dbusa' when c.id LIKE 'reonomy%' THEN 'reonomy' END AS relation_source
    FROM "Zeustra"."public"."companies" c 
    JOIN  "Zeustra"."public"."properties" p
    ON c.source_property_id=p.original_source_id





)
SELECT gen_random_uuid() AS id ,* from companies_to_properties