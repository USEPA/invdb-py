/*================================================================================================
--==================================== STATE REPORT QUERY FUNCTIONS ==============================

Uses the same query logic as the national report query functions, except that it will group by and 
return the state as well. Instead of returning one row/year, it will return +50 rows (= # of states and territories).
The results are still primarily grouped by year, so all data for year 1990 will appear followed by 
all data for year 1991, etc. States are sorted within their groups in ascending order by state code.
--==============================================================================================*/

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CAT_CO2E_SUBSECTOR(input_pub_year_id INTEGER, input_layer_id INTEGER, input_subsector_name TEXT, input_ghg_category_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_sta_co2e_subsector_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_sta_co2e_subsector_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					gwp_version_var, input_ghg_category_name);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the subsector ID based on the input subsector name
		subsectors AS (
	  		SELECT subsector_id
     		FROM ggds_invdb.dim_subsector
     		WHERE subsector_name = input_subsector_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_nat_sta_co2e_subsector_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_nat_sta_co2e_subsector_temp_info ON em.ghg_id = f_em_nat_sta_co2e_subsector_temp_info.ghg_id
         JOIN subsectors on em.sub_sector_id = subsectors.subsector_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_nat_sta_co2e_subsector_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
--  select *
--  from ggds_invdb.F_EM_STA_CAT_CO2E_SUBSECTOR(11, 1, 'Fugitives', 'CH4')

--========================================================================================
--===== same function except filtering by category and ghg category name ================

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CAT_CO2E_CATEGORY(input_pub_year_id INTEGER, input_layer_id INTEGER, input_category_name TEXT, input_ghg_category_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_cat_co2e_category_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_cat_co2e_category_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					gwp_version_var, input_ghg_category_name);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the category ID based on the input category name
		categories AS (
	  		SELECT category_id
     		FROM ggds_invdb.dim_category
     		WHERE category_name = input_category_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_cat_co2e_category_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_cat_co2e_category_temp_info ON em.ghg_id = f_em_sta_cat_co2e_category_temp_info.ghg_id
         JOIN categories on em.category_id = categories.category_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_cat_co2e_category_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
--select *
--from ggds_invdb.F_EM_STA_CAT_CO2E_CATEGORY(11, 1, 'Abandoned Wells', 'CH4');

--========================================================================================
--===== same function except filtering by subsector, category, and ghg category name =====

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CAT_CO2E_SUBSECTOR_CATEGORY(input_pub_year_id INTEGER, input_layer_id INTEGER, input_subsector_name TEXT, input_category_name TEXT, input_ghg_category_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_cat_co2e_subsector_category_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_cat_co2e_subsector_category_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					gwp_version_var, input_ghg_category_name);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the subsector ID based on the input subsector name
		subsectors AS (
	  		SELECT subsector_id
     		FROM ggds_invdb.dim_subsector
     		WHERE subsector_name = input_subsector_name
	 	),
        -- select the category ID based on the input category name
		categories AS (
	  		SELECT category_id
     		FROM ggds_invdb.dim_category
     		WHERE category_name = input_category_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_cat_co2e_subsector_category_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_cat_co2e_subsector_category_temp_info ON em.ghg_id = f_em_sta_cat_co2e_subsector_category_temp_info.ghg_id
         JOIN subsectors on em.sub_sector_id = subsectors.subsector_id
         JOIN categories on em.category_id = categories.category_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_cat_co2e_subsector_category_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_CAT_CO2E_SUBSECTOR_CATEGORY(11, 1, 'Fugitives', 'Abandoned Wells', 'CH4');

--============================================================================================
--===== same function except filtering by category, subcategory 1, and ghg category name =====

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CAT_CO2E_CATEGORY_SUBCATEGORY1(input_pub_year_id INTEGER, input_layer_id INTEGER, input_category_name TEXT, input_subcategory1 TEXT, input_ghg_category_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_cat_co2e_category_subcategory1_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_cat_co2e_category_subcategory1_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					gwp_version_var, input_ghg_category_name);
    			  
  	-- execute and return the main query
    return QUERY
	with 
        -- select the category ID based on the input category name
		categories AS (
	  		SELECT category_id
     		FROM ggds_invdb.dim_category
     		WHERE category_name = input_category_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_cat_co2e_category_subcategory1_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_cat_co2e_category_subcategory1_temp_info ON em.ghg_id = f_em_sta_cat_co2e_category_subcategory1_temp_info.ghg_id
         JOIN categories on em.category_id = categories.category_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
      AND em.sub_category_1 = input_subcategory1
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_cat_co2e_category_subcategory1_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_CAT_CO2E_CATEGORY_SUBCATEGORY1(11, 1, 'Abandoned Wells', 'Abandoned Wells', 'CH4');

--========================================================================================
--============ same function except filtering by sector and ghg long name =============

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_GHG_CO2E_SECTOR(input_pub_year_id INTEGER, input_layer_id INTEGER, input_sector_name TEXT, input_ghg_longname TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_ghg_co2e_sector_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_ghg_co2e_sector_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
					WHERE ghg.ghg_longname = ''%s''',
					gwp_version_var, input_ghg_longname);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the sector ID based on the input sector name
		sectors AS (
	  		SELECT sector_id
     		FROM ggds_invdb.dim_sector
     		WHERE sector_name = input_sector_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_ghg_co2e_sector_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_ghg_co2e_sector_temp_info ON em.ghg_id = f_em_sta_ghg_co2e_sector_temp_info.ghg_id
         JOIN sectors on em.sector_id = sectors.sector_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_ghg_co2e_sector_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_GHG_CO2E_SECTOR(11, 1, 'Energy', 'Methane');

--============================================================================================
--===== same function except filtering by category, subcategory 1, and ghg category name =====

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_GHG_CO2E_CATEGORY(input_pub_year_id INTEGER, input_layer_id INTEGER, input_category_name TEXT, input_ghg_longname TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_ghg_co2e_category_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_ghg_co2e_category_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
					WHERE ghg.ghg_longname = ''%s''',
					gwp_version_var, input_ghg_longname);
    			  
  	-- execute and return the main query
    return QUERY
	with 
        -- select the category ID based on the input category name
		categories AS (
	  		SELECT category_id
     		FROM ggds_invdb.dim_category
     		WHERE category_name = input_category_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_ghg_co2e_category_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_ghg_co2e_category_temp_info ON em.ghg_id = f_em_sta_ghg_co2e_category_temp_info.ghg_id
         JOIN categories on em.category_id = categories.category_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_ghg_co2e_category_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_GHG_CO2E_CATEGORY(11, 1, 'Abandoned Wells', 'Methane');

--========================================================================================
--==================== same function except filtering by category only ===================

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CO2E_SUBSECTOR_ALL(input_pub_year_id INTEGER, input_layer_id INTEGER, input_subsector_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_co2e_subsector_all_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_co2e_subsector_all_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg;',
					gwp_version_var);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the subsector ID based on the input subsector name
		subsectors AS (
	  		SELECT subsector_id
     		FROM ggds_invdb.dim_subsector
     		WHERE subsector_name = input_subsector_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_co2e_subsector_all_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_co2e_subsector_all_temp_info ON em.ghg_id = f_em_sta_co2e_subsector_all_temp_info.ghg_id
         JOIN subsectors on em.sub_sector_id = subsectors.subsector_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_co2e_subsector_all_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_CO2E_SUBSECTOR_ALL(11, 1, 'Fugitives');


--========================================================================================
--==================== same function except filtering by category only ===================

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_STA_CO2E_CATEGORY_ALL(input_pub_year_id INTEGER, input_layer_id INTEGER, input_category_name TEXT) RETURNS TABLE(year INTEGER, geo_ref VARCHAR(20), total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_sta_co2e_category_all_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_sta_co2e_category_all_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg;',
					gwp_version_var);
    			  
  	-- execute and return the main query
    return QUERY
	with 
		-- select the category ID based on the input category name
		categories AS (
	  		SELECT category_id
     		FROM ggds_invdb.dim_category
     		WHERE category_name = input_category_name
	 	)
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, em.geo_ref, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * f_em_sta_co2e_category_all_temp_info.gwp_factor) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_sta_co2e_category_all_temp_info ON em.ghg_id = f_em_sta_co2e_category_all_temp_info.ghg_id
         JOIN categories on em.category_id = categories.category_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
    GROUP BY em.geo_ref, dts.year
    ORDER BY em.geo_ref, dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_sta_co2e_category_all_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.F_EM_STA_CO2E_CATEGORY_ALL(11, 1, 'Abandoned Wells');