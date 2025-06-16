
{{config(post_hook="ALTER TABLE {{this}} add primary key(id)")}}


with all_abbrivations AS
(
SELECT DISTINCT {{dim_name("state")}} as abbrivation
FROM {{ref('reonomy_properties')}}
UNION
SELECT DISTINCT {{dim_name("state")}} as abbrivation
FROM {{ref('dbusa_tenants')}}
),
states_names AS --must run dbt seed before to get the states lookup table
(

    SELECT lookup.name,abrv.abbrivation
    FROM all_abbrivations abrv LEFT JOIN {{source('raw_data','states_lookup')}} lookup
    ON abrv.abbrivation={{dim_name("lookup.standard_abbrviation")}}
	OR 
	abrv.abbrivation={{dim_name("lookup.informal_abbrviation")}}-- in case the source used an informal state abbriviation 


)
select MD5(abbrivation)as id, * from states_names

