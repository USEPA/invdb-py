-- DROP FUNCTION ggds_invdb.f_em_natby_cat_co2e_category(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natby_cat_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	prior_year integer;
begin

prior_year = input_pub_year_id - 1;
	
    RETURN QUERY 
    SELECT cat.year, cat.total_quantity 
    from ggds_invdb.f_em_nat_cat_co2e_category(
   		prior_year, input_layer_id, input_category_name, input_ghg_category_name) cat;
	     
END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_natby_cat_co2e_sector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natby_cat_co2e_sector(input_pub_year_id integer, input_layer_id integer, input_sector_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	prior_year integer;
begin
	prior_year = input_pub_year_id - 1;
   
    RETURN QUERY 
   	 SELECT sec.year, sec.total_quantity 
   	 FROM ggds_invdb.f_em_nat_cat_co2e_sector(
   		prior_year, input_layer_id, input_sector_name, input_ghg_category_name) sec;
END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_natby_cat_co2e_subsector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natby_cat_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	prior_year integer;
begin
	prior_year = input_pub_year_id - 1;
	
    RETURN QUERY 
   	SELECT subsec.year, subsec.total_quantity 
   	FROM ggds_invdb.f_em_nat_cat_co2e_subsector(
   		prior_year,input_layer_id,input_subsector_name,input_ghg_category_name) subsec;

END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_natby_cat_co2e_subsector_category(int4, int4, text, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natby_cat_co2e_subsector_category(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	prior_year integer;
begin
	prior_year = input_pub_year_id - 1;

    RETURN QUERY 
    SELECT ssc.year, ssc.total_quantity 
   	FROM ggds_invdb.f_em_nat_cat_co2e_subsector_category(
   		prior_year,input_layer_id,input_subsector_name,input_category_name,input_ghg_category_name) ssc;
     
END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_natby_ghg_co2e_category(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_natby_ghg_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_longname text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
declare
	prior_year integer;
begin
	prior_year = input_pub_year_id - 1;
	
    RETURN QUERY 
    SELECT ghgcat.year, ghgcat.total_quantity 
   	 FROM ggds_invdb.f_em_nat_ghg_co2e_category(
   		prior_year, input_layer_id, input_category_name, input_ghg_longname) ghgcat;
	 
END
$function$
;

