
{{ config(materialized='table') }}

SELECT * FROM {{source('raw_data','dbusa_tenants')}}
