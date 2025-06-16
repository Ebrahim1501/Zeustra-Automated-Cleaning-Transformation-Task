
      insert into "Zeustra"."public"."all_properties" ("id", "original_source_id", "state", "county", "city", "zip_code", "address", "longitude", "latitude", "accessor_parcel_number_raw", "block_id", "census_tract", "legal_description", "depth", "existing_floor_area_ratio", "frontage", "land_area_acres", "land_area_sqft", "total_building_sqft", "sqft_min", "sqft_max", "land_area_sqft_rounded", "last_modified_date", "last_sale_date", "last_sale_price_per_sqft", "last_sale_price_rounded", "last_transaction_type", "neighborhood_name", "mcd_name", "msa_name", "number_of_buildings", "number_of_floors", "number_of_units", "residential_units", "owner_address", "property_type", "property_subtype", "year_built", "year_renovated", "source_table")
    (
        select "id", "original_source_id", "state", "county", "city", "zip_code", "address", "longitude", "latitude", "accessor_parcel_number_raw", "block_id", "census_tract", "legal_description", "depth", "existing_floor_area_ratio", "frontage", "land_area_acres", "land_area_sqft", "total_building_sqft", "sqft_min", "sqft_max", "land_area_sqft_rounded", "last_modified_date", "last_sale_date", "last_sale_price_per_sqft", "last_sale_price_rounded", "last_transaction_type", "neighborhood_name", "mcd_name", "msa_name", "number_of_buildings", "number_of_floors", "number_of_units", "residential_units", "owner_address", "property_type", "property_subtype", "year_built", "year_renovated", "source_table"
        from "all_properties__dbt_tmp180455328023"
    )


  