/*========================================================================================
The following query functions are used by national report formulas as called in column C for any 
national report excel file. 

The first function, F_EM_NAT_CAT_CO2E_SECTOR, takes a sector name (hence the "SECTOR" suffix)
and a GHG category name (hence the "_CAT_" name part). Note that the pub_year_id and layer_id are added as parameters 
to the functions as well; these are supplied by the metadata of the national report file. The function returns a table 
that populates one row of data in the national report's Query_Results tab corresponding to the row where the function is called.
The function returns a table with columns 'year' and 'total_quantity'. 'year' values range in ascending order between 1990 
and the max time series of the national report's publication year, and 'total_quantity' contains the sum of the selected values 
(filtered by the parameter values, e.g. sector name and ghg category name) from the facts archive/emissions_key table data multiplied 
by their appropriate GWP factors.
========================================================================================*/ 


CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_cat_co2e_sector(input_pub_year_id integer, input_layer_id integer, input_sector_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;

	gwp_column = COALESCE(gwp_column_select, gwp_version_var);

	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.sector_name = $4
						and rp.ghg_category_name = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_sector_name, input_ghg_category_name; 
END
$function$
;

-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CAT_CO2E_SECTOR(11, 1, 'Energy', 'CH4');

--========================================================================================
--===== same function except filtering by subsector and ghg category name ================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_cat_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);

	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.ghg_category_name = $4
						and rp.subsector_name = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	 --Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_ghg_category_name, input_subsector_name;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CAT_CO2E_SUBSECTOR(11, 1, 'Fugitives', 'CH4');

--========================================================================================
--===== same function except filtering by category and ghg category name ================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_cat_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_category_name text, gwp_column_select text DEFAULT NULL::text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
	str_query text;
begin
	IF gwp_column_select IS NULL THEN
		RETURN QUERY 
		select rencceca.year, total_value from ggds_invdb.rollup_em_nat_cat_co2e_category_aggregate rencceca 
		where pub_year_id = input_pub_year_id 
			and layer_id = input_layer_id
			and category_name = input_category_name
			and ghg_category_name = input_ghg_category_name;
	ELSE

		-- select the GWP version and max time series year ID based on the input publication year ID
		SELECT dts.year_id into max_year_id_var 
		FROM ggds_invdb.dim_time_series dts
		JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
		where dpy.pub_year_id = input_pub_year_id;
	
		str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_value
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.ghg_category_name = $4
						and rp.category_name = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column_select);
	
		-- Execute the dynamic SQL query and return the result
		RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_ghg_category_name, input_category_name;
	end IF;
	     
END
$function$
;


-- test with results
--select *
--from ggds_invdb.F_EM_NAT_CAT_CO2E_CATEGORY(11, 1, 'Abandoned Wells', 'CH4');

--========================================================================================
--===== same function except filtering by subsector, category, and ghg category name =====

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_cat_co2e_subsector_category(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
			
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);

    str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.ghg_category_name = $4
						and rp.subsector_name = $5
						and rp.category_name = $6
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	 --Execute the dynamic SQL query and return the result
     RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_ghg_category_name, input_subsector_name, input_category_name;
     
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CAT_CO2E_SUBSECTOR_CATEGORY(11, 1, 'Fugitives', 'Abandoned Wells', 'CH4');

--============================================================================================
--===== same function except filtering by category, subcategory 1, and ghg category name =====

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_cat_co2e_category_subcategory1(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_subcategory1 text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
		
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.ghg_category_name = $4
						and rp.category_name = $5
						and rp.sub_category_1 = $6
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
  
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_ghg_category_name, input_category_name, input_subcategory1;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CAT_CO2E_CATEGORY_SUBCATEGORY1(11, 1, 'Abandoned Wells', 'Abandoned Wells', 'CH4');

--========================================================================================
--============ same function except filtering by subsector and ghg long name =============

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_longname text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
	
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.subsector_name = $4
						and rp.ghg_longname = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_subsector_name, input_ghg_longname;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_GHG_CO2E_SUBSECTOR(11, 1, 'Fugitives', 'Methane');

--===================================================================================
--===== same function except filtering by category and ghg long name ================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_longname text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
	
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						AND rp.category_name = $4
						and rp.ghg_longname = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_category_name, input_ghg_longname;
	 
END
$function$
;


-- test with results
--select *
--from ggds_invdb.F_EM_NAT_GHG_CO2E_CATEGORY(11, 1, 'Abandoned Wells', 'Methane');

--==================================================================================================
--=== same function except filtering by category, fuel1 (which supports NULL), and ghg long name === 

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_category_fuel1(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_fuel_type_name text DEFAULT NULL::text, input_ghg_longname text DEFAULT NULL::text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
    fuel_type_where_clause text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(dpy.gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year dpy
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_ghg_co2e_category_fuel1_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_ghg_co2e_category_fuel1_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
					WHERE ghg.ghg_longname = ''%s''',
					CASE WHEN gwp_column_select IS NULL THEN gwp_version_var ELSE gwp_column_select END, input_ghg_longname);
    			  
  	-- execute and return the main query
    IF input_fuel_type_name IS NOT NULL
    THEN
        RETURN QUERY
        WITH 
            -- select the category ID based on the input category name
            categories AS (
                SELECT category_id
                FROM ggds_invdb.dim_category
                WHERE category_name = input_category_name
            ),
            fuels AS (
                SELECT fuel_type_id
                FROM ggds_invdb.dim_fuel_type
                WHERE fuel_type_name = input_fuel_type_name
            )
        -- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * COALESCE(f_em_nat_ghg_co2e_category_fuel1_temp_info.gwp_factor, 1)) AS total_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN f_em_nat_ghg_co2e_category_fuel1_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_category_fuel1_temp_info.ghg_id
            JOIN categories on em.category_id = categories.category_id
            JOIN fuels on em.fuel_type_id_1 = fuels.fuel_type_id
            JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
        WHERE fa.year_id <= max_year_id_var
			AND fa.pub_year_id = input_pub_year_id
			AND fa.layer_id = input_layer_id 
        GROUP BY dts.year
        ORDER BY dts.year ASC;
    ELSE -- if input_fuel_type_name IS NULL
        RETURN QUERY
        WITH 
            -- select the category ID based on the input category name
            categories AS (
                SELECT category_id
                FROM ggds_invdb.dim_category
                WHERE category_name = input_category_name
            )
        -- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
        SELECT dts.year::integer, SUM(fa.value::numeric * COALESCE(f_em_nat_ghg_co2e_category_fuel1_temp_info.gwp_factor, 1)) AS total_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN f_em_nat_ghg_co2e_category_fuel1_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_category_fuel1_temp_info.ghg_id
            JOIN categories on em.category_id = categories.category_id
            JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
        WHERE fa.year_id <= max_year_id_var
			AND fa.pub_year_id = input_pub_year_id
			AND fa.layer_id = input_layer_id 
			AND fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' --only include numeric values in the aggregation
			AND em.fuel_type_id_1 IS NULL
        GROUP BY dts.year
        ORDER BY dts.year ASC;
    END IF;
  
    DROP TABLE IF EXISTS f_em_nat_ghg_co2e_category_fuel1_temp_info;
     
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_GHG_CO2E_CATEGORY_FUEL1(11, 1, 'Abandoned Wells', NULL, 'Methane');

--==================================================================================================
--=== same function except filtering by category, fuel2 (which supports NULL), and ghg category name === 

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_category_fuel2(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_fuel2_type_name text DEFAULT NULL::text, input_ghg_category_name text DEFAULT NULL::text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
    fuel_type_where_clause text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(dpy.gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year dpy
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_ghg_co2e_category_fuel2_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_ghg_co2e_category_fuel2_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					CASE WHEN gwp_column_select IS NULL THEN gwp_version_var ELSE gwp_column_select END, input_ghg_category_name);
    			  
  	-- execute and return the main query
    IF input_fuel2_type_name IS NOT NULL
    THEN
        RETURN QUERY
        WITH 
            -- select the category ID based on the input category name
            categories AS (
                SELECT category_id
                FROM ggds_invdb.dim_category
                WHERE category_name = input_category_name
            ),
            fuels AS (
                SELECT fuel_type_id
                FROM ggds_invdb.dim_fuel_type
                WHERE fuel_type_name = input_fuel2_type_name
            )
        -- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * COALESCE(f_em_nat_ghg_co2e_category_fuel2_temp_info.gwp_factor, 1)) AS total_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN f_em_nat_ghg_co2e_category_fuel2_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_category_fuel2_temp_info.ghg_id
            JOIN categories on em.category_id = categories.category_id
            JOIN fuels on em.fuel_type_id_2 = fuels.fuel_type_id
            JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
        WHERE fa.year_id <= max_year_id_var
			AND fa.pub_year_id = input_pub_year_id
			AND fa.layer_id = input_layer_id 
        GROUP BY dts.year
        ORDER BY dts.year ASC;
    ELSE -- if input_fuel2_type_name IS NULL
        RETURN QUERY
        WITH 
            -- select the category ID based on the input category name
            categories AS (
                SELECT category_id
                FROM ggds_invdb.dim_category
                WHERE category_name = input_category_name
            )
        -- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
        SELECT dts.year::integer, SUM(fa.value::numeric * COALESCE(f_em_nat_ghg_co2e_category_fuel2_temp_info.gwp_factor, 1)) AS total_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN f_em_nat_ghg_co2e_category_fuel2_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_category_fuel2_temp_info.ghg_id
            JOIN categories on em.category_id = categories.category_id
            JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
        WHERE fa.year_id <= max_year_id_var
			AND fa.pub_year_id = input_pub_year_id
			AND fa.layer_id = input_layer_id 
			AND fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' --only include numeric values in the aggregation
			AND em.fuel_type_id_2 IS NULL
        GROUP BY dts.year
        ORDER BY dts.year ASC;
    END IF;
  
    DROP TABLE IF EXISTS f_em_nat_ghg_co2e_category_fuel2_temp_info;
     
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_GHG_CO2E_CATEGORY_FUEL2(11, 1, 'Biomass--CO2','Ethanol','CO2');

--========================================================================================
--============ same function except filtering by subsector and ghg long name =============

CREATE OR REPLACE FUNCTION ggds_invdb.F_EM_NAT_GHG_CO2E_SUBCATEGORY1(input_pub_year_id INTEGER, input_layer_id INTEGER, input_subcategory1 TEXT, input_ghg_longname TEXT, gwp_column_select text default null) RETURNS TABLE(year INTEGER, total_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var text;
	max_year_id_var integer;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(dpy.gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year dpy
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_ghg_co2e_subcategory1_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_ghg_co2e_subcategory1_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
					WHERE ghg.ghg_longname = ''%s''',
					CASE WHEN gwp_column_select IS NULL THEN gwp_version_var ELSE gwp_column_select END, input_ghg_longname);
    			  
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * COALESCE(f_em_nat_ghg_co2e_subcategory1_temp_info.gwp_factor, 1)) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_nat_ghg_co2e_subcategory1_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_subcategory1_temp_info.ghg_id
         JOIN ggds_invdb.DIM_TIME_SERIES dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
      AND em.sub_category_1 = input_subcategory1
    GROUP BY dts.year
    ORDER BY dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_nat_ghg_co2e_subcategory1_temp_info;
     
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
--  select *
--  from ggds_invdb.F_EM_NAT_GHG_CO2E_SUBCATEGORY1(11, 1, 'Abandoned Wells', 'Methane');

--========================================================================================
--================== same function except filtering by subsector only ====================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_co2e_sector_all(input_pub_year_id integer, input_layer_id integer, input_sector_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
    			  
  	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.sector_name = $4
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_sector_name;     
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CO2E_SECTOR_ALL(11, 1, 'Energy');

--========================================================================================
--================== same function except filtering by subsector only ====================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_co2e_subsector_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
		
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.subsector_name = $4
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
  
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_subsector_name;
		
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CO2E_SUBSECTOR_ALL(11, 1, 'Fugitives');

--========================================================================================
--==================== same function except filtering by category only ===================

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_co2e_category_all(input_pub_year_id integer, input_layer_id integer, input_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
    			  
  	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.category_name = $4
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_category_name;
     
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CO2E_CATEGORY_ALL(11, 1, 'Abandoned Wells');

--=========================================================================================
--============= same function except filtering by subsector and category only =============

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_co2e_subsector_category_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
	
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.subsector_name = $4
						and rp.category_name = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_subsector_name, input_category_name; 
	
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CO2E_SUBSECTOR_CATEGORY_ALL(11, 1, 'Fugitives', 'Abandoned Wells');

--=========================================================================================
--============= same function except filtering by subsector and category only =============

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_co2e_subsector_subcategory1_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_subcategory1 text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
		
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
	
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.subsector_name = $4
						and rp.sub_category_1 = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
  
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_subsector_name, input_subcategory1;
		
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NAT_CO2E_SUBSECTOR_SUBCATEGORY1_ALL(11, 1, 'Fugitives', 'Abandoned Wells');


--=========================================================================================
--============= same function except filtering by ghg and subcategory1 only =============

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_subcategory(input_pub_year_id integer, input_layer_id integer, input_subcategory1 text, input_ghg_longname text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	str_query text;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	SELECT LOWER(dpy.gwp_column), dts.year_id into gwp_version_var, max_year_id_var
	 FROM ggds_invdb.dim_time_series dts
	 JOIN ggds_invdb.dim_publication_year dpy on dpy.max_time_series = dts.year
     where dpy.pub_year_id = input_pub_year_id;
	
	gwp_column = COALESCE(gwp_column_select, gwp_version_var);
	
	str_query = format('SELECT rp.year, SUM(rp.value * COALESCE(rp.%I, 1)) AS total_quantity
   					from ggds_invdb.rollup_em_co2e rp
					WHERE rp.year_id <= $1
	  					AND rp.pub_year_id = $2
      					AND rp.layer_id = $3
						and rp.sub_category_1 = $4
						and rp.ghg_longname = $5
					GROUP BY rp.year
    				ORDER BY rp.year ASC',
					gwp_column);
   
	-- Execute the dynamic SQL query and return the result
    RETURN QUERY EXECUTE str_query using max_year_id_var, input_pub_year_id, input_layer_id, input_subcategory1, input_ghg_longname; 
		
END
$function$
;


--=========================================================================================
--============= same function except filtering by ghg and subcategory1 only =============

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_ghg_co2e_subcategory1(input_pub_year_id integer, input_layer_id integer, input_subcategory1 text, input_ghg_longname text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(dpy.gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year dpy
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_ghg_co2e_subcategory1_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_ghg_co2e_subcategory1_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
					WHERE ghg.ghg_longname = ''%s''',
					CASE WHEN gwp_column_select IS NULL THEN gwp_version_var ELSE gwp_column_select END, input_ghg_longname);
    			  
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT dts.year::integer, SUM(CASE WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric ELSE 0 END * COALESCE(f_em_nat_ghg_co2e_subcategory1_temp_info.gwp_factor, 1)) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_nat_ghg_co2e_subcategory1_temp_info ON em.ghg_id = f_em_nat_ghg_co2e_subcategory1_temp_info.ghg_id
         JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_year_id_var
	  AND fa.pub_year_id = input_pub_year_id
      AND fa.layer_id = input_layer_id 
      AND em.sub_category_1 = input_subcategory1
    GROUP BY dts.year
    ORDER BY dts.year ASC;
  
    DROP TABLE IF EXISTS f_em_nat_ghg_co2e_subcategory1_temp_info;
     
END
$function$
;

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_sta_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, geo_ref character varying, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_pub_year_id_var integer;
	gwp_column text;
begin
	-- select the GWP version and max time series year ID based on the input publication year ID
	with pub_year_info AS (
		SELECT LOWER(dpy.gwp_column) AS gwp_version, max_time_series 
		FROM ggds_invdb.dim_publication_year dpy
		WHERE pub_year_id = input_pub_year_id
	)
	SELECT pub_year_info.gwp_version, dts.year_id into gwp_version_var, max_pub_year_id_var
	FROM ggds_invdb.dim_time_series dts
		JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
		
	-- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
	DROP TABLE IF EXISTS f_em_nat_sta_co2e_subsector_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE f_em_nat_sta_co2e_subsector_temp_info AS
					SELECT ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
						JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg.ghg_category_id = ghg_cat.ghg_category_id
					WHERE ghg_cat.ghg_category_name = ''%s''',
					CASE WHEN gwp_column_select IS NULL THEN gwp_version_var ELSE gwp_column_select END, input_ghg_category_name);
    			  
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
	SELECT dts.year::integer, em.geo_ref, SUM(fa.value::numeric * COALESCE(f_em_nat_sta_co2e_subsector_temp_info.gwp_factor, 1)) AS total_quantity
    FROM ggds_invdb.emissions_key em 
         JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
         JOIN f_em_nat_sta_co2e_subsector_temp_info ON em.ghg_id = f_em_nat_sta_co2e_subsector_temp_info.ghg_id
         JOIN subsectors on em.sub_sector_id = subsectors.subsector_id
         JOIN ggds_invdb.dim_time_series dts on fa.year_id = dts.year_id
    WHERE fa.year_id <= max_pub_year_id_var
      AND fa.layer_id = input_layer_id 
      AND fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' --only include numeric values in the aggregation
    GROUP BY dts.year, em.geo_ref
    ORDER BY dts.year, em.geo_ref ASC;
  
    DROP TABLE IF EXISTS f_em_nat_sta_co2e_subsector_temp_info;
     
END
$function$
;
