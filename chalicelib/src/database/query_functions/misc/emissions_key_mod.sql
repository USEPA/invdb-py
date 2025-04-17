-- (putting this here as a record of what I ran on test for when we need to do it on UAT)

-- add ghg_category column to emissions_key table
CREATE TABLE ggds_invdb.new_emissions_key AS
SELECT emissions_key_id, emissions_uid, sector_id, sub_sector_id, category_id, 
	sub_category_1, sub_category_2, sub_category_3, sub_category_4, sub_category_5, 
	carbon_pool, fuel_type_id_1, fuel_type_id_2, geo_ref, "EXCLUDE", crt_code, 
	id, cbi_activity, units, NULL::VARCHAR(50) AS ghg_category, ghg_id, gwp, source_file_id
FROM ggds_invdb.emissions_key
WHERE false;

INSERT into ggds_invdb.new_emissions_key (emissions_key_id, emissions_uid, sector_id, sub_sector_id, category_id, 
	sub_category_1, sub_category_2, sub_category_3, sub_category_4, sub_category_5, 
	carbon_pool, fuel_type_id_1, fuel_type_id_2, geo_ref, "EXCLUDE", crt_code, 
	id, cbi_activity, units, ghg_category, ghg_id, gwp, source_file_id)
SELECT emissions_key_id, emissions_uid, sector_id, sub_sector_id, category_id, 
	sub_category_1, sub_category_2, sub_category_3, sub_category_4, sub_category_5, 
	carbon_pool, fuel_type_id_1, fuel_type_id_2, geo_ref, "EXCLUDE", crt_code, 
	id, cbi_activity, units, NULL, ghg_id, gwp, source_file_id
FROM ggds_invdb.emissions_key;

DROP TABLE ggds_invdb.emissions_key;
ALTER TABLE ggds_invdb.new_emissions_key RENAME TO emissions_key;
