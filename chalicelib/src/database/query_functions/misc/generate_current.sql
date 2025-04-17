CREATE OR REPLACE FUNCTION ggds_invdb.generate_current_dataset_as_landscape_json(input_pub_year_id integer, input_layer_id integer, input_user_id integer)
RETURNS JSON
 LANGUAGE plpgsql
AS $function$
declare 
    max_time_series INTEGER;
    time_series_column_list_str TEXT;
    time_series_column_select_list_str TEXT;
    sql_statement TEXT;
    pub_version_id_var INTEGER;
    publication_year_var INTEGER;
    publication_id_var INTEGER;
    pub_object_id_var INTEGER;
    raw_results JSON;
    refined_results JSON;
begin
    -- generate the snapshot data
    DROP TABLE IF EXISTS generate_current_dataset_temp_raw_data;
	CREATE TEMP TABLE generate_current_dataset_temp_raw_data AS
    SELECT * FROM ggds_invdb.em_sta_all_snapshot(input_pub_year_id, input_layer_id);
 
    -- transform the raw data here (foreign keys and landscape conversion)
    -- decode the dim table IDs to their string names (for sector, subsector, category, fuel1, fuel2, ghg)
    DROP TABLE IF EXISTS generate_current_dataset_temp_raw_data_portrait;
	CREATE TEMP TABLE generate_current_dataset_temp_raw_data_portrait AS
    SELECT MD5(format('(''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'')', 
                sectors.sector_name,
                subsectors.subsector_name,
                categories.category_name,
                imm.sub_category_1,
                imm.sub_category_2,
                imm.sub_category_3,
                imm.sub_category_4,
                imm.sub_category_5,
                imm.carbon_pool,
                fuels1.fuel_type_name,
                fuels2.fuel_type_name,
                imm.geo_ref,
                imm.exclude,
                imm.crt_code,
                imm.id,
                imm.cbi_activity,
                imm.units,
                ghgs.ghg_longname))::TEXT as data_key,
           sectors.sector_name as sector,
           subsectors.subsector_name as subsector,
           categories.category_name as category,
           imm.sub_category_1,
           imm.sub_category_2,
           imm.sub_category_3,
           imm.sub_category_4,
           imm.sub_category_5,
           imm.carbon_pool,
           fuels1.fuel_type_name as fuel1,
           fuels2.fuel_type_name as fuel2,
           imm.exclude,
           imm.crt_code,
           imm.id,
           imm.cbi_activity,
           imm.units,
           ghgs.ghg_longname as ghg,
           ghg_categories.ghg_category_name as ghg_category,
           imm.year,
           imm.weighted_quantity
    FROM generate_current_dataset_temp_raw_data imm
         LEFT JOIN ggds_invdb.dim_sector sectors ON sectors.sector_id = imm.sector
         LEFT JOIN ggds_invdb.dim_subsector subsectors ON subsectors.subsector_id = imm.subsector
         LEFT JOIN ggds_invdb.dim_category categories ON categories.category_id = imm.category
         LEFT JOIN ggds_invdb.dim_fuel_type fuels1 ON fuels1.fuel_type_id = imm.fuel1
         LEFT JOIN ggds_invdb.dim_fuel_type fuels2 ON fuels2.fuel_type_id = imm.fuel2
         LEFT JOIN ggds_invdb.dim_ghg ghgs ON ghgs.ghg_id = imm.ghg
         LEFT JOIN ggds_invdb.dim_ghg_category ghg_categories ON ghg_categories.ghg_category_id = ghgs.ghg_category_id;

    --========================= PERFORM THE DATA PIVOT =======================
    -- fetch the max time series
    SELECT py.max_time_series INTO max_time_series
    FROM ggds_invdb.dim_publication_year py
    WHERE pub_year_id = input_pub_year_id;

    -- generate the time series column list string
    time_series_column_list_str := '';
    time_series_column_select_list_str := '';
    FOR year_value IN 1990..max_time_series
    LOOP
        time_series_column_list_str = time_series_column_list_str || format('"Y%s" NUMERIC, ', year_value);
        time_series_column_select_list_str = time_series_column_select_list_str || format('"Y%s", ', year_value);
    END LOOP;
    time_series_column_list_str := substr(time_series_column_list_str, 1, length(time_series_column_list_str) - 2);
    time_series_column_select_list_str := substr(time_series_column_select_list_str, 1, length(time_series_column_select_list_str) - 2);

    -- create immediate table used to pivot the raw data
    DROP TABLE IF EXISTS ggds_invdb.generate_current_dataset_temp_raw_data_landscape_imm;
    sql_statement := '
        CREATE TABLE ggds_invdb.generate_current_dataset_temp_raw_data_landscape_imm AS
        SELECT *
        FROM crosstab(
            ''SELECT data_key::TEXT, year, weighted_quantity FROM generate_current_dataset_temp_raw_data_portrait ORDER BY data_key, year'',
            ''SELECT DISTINCT year FROM generate_current_dataset_temp_raw_data_portrait ORDER BY year''
        ) AS ct (
            data_key TEXT, ' || time_series_column_list_str || '
        );';
    -- Execute the dynamic SQL statement
    EXECUTE sql_statement;

    -- create the landscape raw data set
    DROP TABLE IF EXISTS ggds_invdb.generate_current_dataset_temp_raw_data_landscape;
    sql_statement = '
    CREATE TABLE ggds_invdb.generate_current_dataset_temp_raw_data_landscape AS (
    SELECT DISTINCT
            info.data_key,
            info.sector,
            info.subsector,
            info.category,
            info.sub_category_1,
            info.sub_category_2,
            info.sub_category_3,
            info.sub_category_4,
            info.sub_category_5,
            info.carbon_pool,
            info.fuel1,
            info.fuel2,
            info.exclude,
            info.crt_code,
            info.id,
            info.cbi_activity,
            info.units,
            info.ghg,
            info.ghg_category,
            ' || time_series_column_select_list_str || '
            FROM generate_current_dataset_temp_raw_data_portrait info 
            JOIN ggds_invdb.generate_current_dataset_temp_raw_data_landscape_imm imm ON info.data_key::TEXT = imm.data_key::TEXT
    );';
    EXECUTE sql_statement;
    --========================= DATA PIVOT COMPLETE ==========================

    -- move raw data into a json
    EXECUTE 'SELECT json_agg(landscape_data)
             FROM (
                SELECT *
                FROM ggds_invdb.generate_current_dataset_temp_raw_data_landscape
             ) landscape_data'
    INTO raw_results;

    -- get the publication year relating to the given pub_year_id
    SELECT pub_year INTO publication_year_var
    FROM ggds_invdb.dim_publication_year years
    WHERE pub_year_id = input_pub_year_id;

    -- get the pub_version_id of the publication version that will hold the 
    -- raw data within the publication_object table
    SELECT pub_version_id INTO pub_version_id_var
    FROM ggds_invdb.publication_version
    WHERE pub_year = publication_year_var
          AND layer_id = input_layer_id
    ORDER BY version_name
    LIMIT 1;

    -- if the appropriate publication version doesn't exist, create it
    IF pub_version_id_var IS NULL 
    THEN
        SELECT ggds_invdb.publication_create_version(publication_year_var, input_layer_id, 'draft', input_user_id)
        INTO pub_version_id_var;
    END IF;

    -- get the publication_id of the script being executed
    SELECT publication_id INTO publication_id_var
    FROM ggds_invdb.dim_publication 
    WHERE row_prefix = CASE WHEN input_layer_id = 1 THEN 'EM_Nat_All' ELSE 'EM_Sta_All' END;

    -- write the raw data into the appropriate publication object
    UPDATE ggds_invdb.publication_object
    SET raw_data = raw_results, last_import_date = CURRENT_TIMESTAMP
    WHERE pub_version_id = pub_version_id_var 
          AND pub_id = publication_id_var
    RETURNING pub_object_id INTO pub_object_id_var;

    -- pass that returned pub_object_id to the refining/redacting function
    SELECT ggds_invdb.em_sta_all_refined(pub_object_id_var) INTO refined_results;
   
    -- write the refined data into that same publication object
    UPDATE ggds_invdb.publication_object
    SET refined_data = refined_results, last_refined_date = CURRENT_TIMESTAMP
    WHERE pub_object_id = pub_object_id_var;

    -- additionally return the refined result to the caller
    RETURN refined_results;

    DROP TABLE IF EXISTS generate_current_dataset_temp_raw_data;
    DROP TABLE IF EXISTS generate_current_dataset_temp_raw_data_portrait;
    DROP TABLE IF EXISTS ggds_invdb.generate_current_dataset_temp_raw_data_landscape_imm;
    DROP TABLE IF EXISTS ggds_invdb.generate_current_dataset_temp_raw_data_landscape;
END
$function$;

select pub_id, pub_object_id, pub_version_id, last_import_date, last_refined_date
from ggds_invdb.publication_object 
order by last_import_date DESC;

select * from ggds_invdb.generate_current_dataset_as_landscape_json(12, 1, 4);
