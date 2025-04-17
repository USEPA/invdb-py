/*========================================================================================
The following query functions are used by national QC report formulas as called in online QC reports

The first function, F_EM_NATQC_CAT_CO2E_CATEGORY, takes a category name 
and a GHG category name (note that the pub_year_id and layer_id are added as parameters 
to the functions as well). The function returns a table 
that populates one row of QC data from the facts_archive table.
The function returns a table with columns 'year' and 'total_quantity'. 'year' values range in ascending order between 1990 
and the max time series of the input pub year ID, and 'total_quantity' contains the sum of the selected values 
(filtered by the parameter values, e.g. sector name and ghg category name) from the facts_archive/emissionsqc_key table data.
QC Data stored in facts_archive is already in CO2e units, so the GWP factors do not need to be applied to the emissions values. 
========================================================================================*/ 

--===== filtering QC data by category and ghg category name ================

-- DROP FUNCTION ggds_invdb.f_em_natqc_cat_co2e_category(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_cat_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;

	-- execute and return the main query
	return 
	QUERY
		SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
		WHERE rp.year_id <= max_year_id_var
			AND rp.pub_year_id = input_pub_year_id
			AND rp.layer_id = input_layer_id 
			AND rp.category_name = input_category_name
			AND rp.GHG_Category_Name = input_ghg_category_name
		GROUP BY rp.year
		ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CAT_CO2E_CATEGORY(11, 1, 'Abandoned Wells', 'CH4');


--========================================================================================
--===== same function except filtering by subsector and ghg category name ================

-- DROP FUNCTION ggds_invdb.f_em_natqc_cat_co2e_subsector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_cat_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
		
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
    WHERE  rp.year_id <= max_year_id_var
		AND rp.pub_year_id = input_pub_year_id
		AND rp.layer_id = input_layer_id 
	  	AND rp.subsector_name = input_subsector_name
	  	AND rp.GHG_Category_Name = input_ghg_category_name
    GROUP BY rp.year
    ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CAT_CO2E_SUBSECTOR(11, 1, 'Non-Energy Use of Fuels', 'CO2');


--========================================================================================
--===== same function except filtering by subsector, category, and ghg category name =====

-- DROP FUNCTION ggds_invdb.f_em_natqc_cat_co2e_subsector_category(int4, int4, text, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_cat_co2e_subsector_category(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
		
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
    WHERE  rp.year_id <= max_year_id_var
		AND rp.pub_year_id = input_pub_year_id
		AND rp.layer_id = input_layer_id 
	  	AND rp.subsector_name = input_subsector_name
		AND rp.category_name = input_category_name
	  	AND rp.GHG_Category_Name = input_ghg_category_name
    GROUP BY rp.year
    ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CAT_CO2E_SUBSECTOR_CATEGORY(11, 1, 'Fugitives', 'Abandoned Wells', 'CH4');


--========================================================================================
--==================== same function except filtering by category only ===================

-- DROP FUNCTION ggds_invdb.f_em_natqc_co2e_category_all(int4, int4, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_co2e_category_all(input_pub_year_id integer, input_layer_id integer, input_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
		
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
    WHERE  rp.year_id <= max_year_id_var
		AND rp.pub_year_id = input_pub_year_id
		AND rp.layer_id = input_layer_id 
		AND rp.category_name = input_category_name
    GROUP BY rp.year
    ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CO2E_CATEGORY_ALL(11, 1, 'Abandoned Wells');


--=========================================================================================
--============= same function except filtering by subsector and category only =============

-- DROP FUNCTION ggds_invdb.f_em_natqc_co2e_subsector_category_all(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_co2e_subsector_category_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
		
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
    WHERE  rp.year_id <= max_year_id_var
		AND rp.pub_year_id = input_pub_year_id
		AND rp.layer_id = input_layer_id 
		AND rp.subsector_name = input_subsector_name
		AND rp.category_name = input_category_name
    GROUP BY rp.year
    ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CO2E_SUBSECTOR_CATEGORY_ALL(11, 1, 'Fugitives', 'Abandoned Wells');


--=========================================================================================
--============= same function except filtering by subsector and category only =============

-- DROP FUNCTION ggds_invdb.f_em_natqc_cat_co2e_sector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_cat_co2e_sector(input_pub_year_id integer, input_layer_id integer, input_sector_name text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
		
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
    WHERE  rp.year_id <= max_year_id_var
		AND rp.pub_year_id = input_pub_year_id
		AND rp.layer_id = input_layer_id 
		AND rp.sector_name = input_sector_name
		AND rp.ghg_category_name = input_ghg_category_name
    GROUP BY rp.year
    ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CAT_CO2E_SECTOR(11, 1, 'Energy', 'CO2');


--============================================================================================
--===== same function except filtering by category, subcategory 1, and ghg category name =====

-- DROP FUNCTION ggds_invdb.f_em_natqc_cat_co2e_category_subcategory1(int4, int4, text, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_cat_co2e_category_subcategory1(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_subcategory1 text, input_ghg_category_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
    			  
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
		WHERE rp.year_id <= max_year_id_var
			AND rp.pub_year_id = input_pub_year_id
			AND rp.layer_id = input_layer_id 
			AND rp.category_name = input_category_name
			AND rp.GHG_Category_Name = input_ghg_category_name
			and rp.subcategory_1 = input_subcategory1
		GROUP BY rp.year
		ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CAT_CO2E_CATEGORY_SUBCATEGORY1(11, 1, 'Abandoned Wells', 'Abandoned Wells', 'CH4');

--========================================================================================
--================== same function except filtering by subsector only ====================

-- DROP FUNCTION ggds_invdb.f_em_natqc_co2e_subsector_all(int4, int4, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_co2e_subsector_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
    			  
  	-- execute and return the main query
    return QUERY
 	-- return each data year up to the max time series and its cumulative value from the facts_archive table for the input layer ID
	SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
		WHERE rp.year_id <= max_year_id_var
			AND rp.pub_year_id = input_pub_year_id
			AND rp.layer_id = input_layer_id 
			AND rp.subsector_name = input_subsector_name
		GROUP BY rp.year
		ORDER BY rp.year ASC;
END
$function$
;


-- test with results
-- select *
-- from ggds_invdb.F_EM_NATQC_CO2E_SUBSECTOR_ALL(11, 1, 'Fugitives');


--========================================================================================
--================== same function except filtering by category and ghg_longname only ====================

-- DROP FUNCTION ggds_invdb.f_em_natqc_ghg_co2e_category(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natqc_ghg_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_longname text, gwp_column_select text default null)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	gwp_version_var text;
	max_year_id_var integer;
begin
	-- select the max time series year ID based on the input publication year ID
	SELECT dts.year_id into max_year_id_var
	FROM ggds_invdb.dim_publication_year dpy
		JOIN ggds_invdb.dim_time_series dts on dpy.max_time_series = dts.year
	WHERE dpy.pub_year_id = input_pub_year_id;
	
	-- execute and return the main query
	return 
	QUERY
		SELECT rp.year, SUM(rp.value) AS total_quantity
		FROM ggds_invdb.rollup_em_natqc_co2e rp 
		WHERE rp.year_id <= max_year_id_var
			AND rp.pub_year_id = input_pub_year_id
			AND rp.layer_id = input_layer_id 
			AND rp.category_name = input_category_name
			AND rp.ghg_longname = input_ghg_longname
		GROUP BY rp.year
		ORDER BY rp.year ASC;
     
END
$function$
;
