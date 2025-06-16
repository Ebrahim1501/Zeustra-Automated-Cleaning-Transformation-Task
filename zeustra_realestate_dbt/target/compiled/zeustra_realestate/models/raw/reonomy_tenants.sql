


WITH occupant_companies AS (
  SELECT DISTINCT
    reonomy_id AS property_id,
    occupant_tenant ->> 'business_name' AS company_name,
    occupant_tenant ->> 'tradestyle_name' AS company_name_2,

    occupant_tenant ->> 'year_started' AS year_started,
    occupant_tenant ->> 'website' AS website,
    occupant_tenant ->> 'employee_total' AS employee_total,
    occupant_tenant ->> 'type_of_location' AS type_of_location,
    occupant_tenant ->> 'sic' AS sic_code,
    occupant_tenant ->> 'sic_description' AS sic_description,
    occupant_tenant ->> 'naics' AS naics_code,
    occupant_tenant ->> 'naics_description' AS naics_description,
    occupant_tenant ->> 'telephone_number' AS company_phone,
    occupant_tenant ->> 'state' AS state,
    occupant_tenant ->> 'city' AS city,
    occupant_tenant ->> 'zip_code' AS zip_code,
    occupant_tenant ->> 'street_address' AS street_address,
    occupant_tenant ->> 'sales_volume' AS sales_volume
  FROM "Zeustra"."public"."reonomy_properties",
  LATERAL jsonb_array_elements(occupant_tenants) AS occupant_tenant
)
SELECT * FROM occupant_companies