-- DROP FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_category(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_category(input_pub_year_id integer, input_layer_id integer, input_category_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
begin

    -- Return the difference between the current and previous year's total values
    RETURN QUERY
    SELECT
        current_cat.year,
       (current_cat.total_quantity - COALESCE(prev_cat.total_quantity, 0)) as total_quantity
    FROM
        ggds_invdb.f_em_nat_cat_co2e_category(
            input_pub_year_id, input_layer_id, input_category_name, input_ghg_category_name
        ) current_cat
    LEFT JOIN
        ggds_invdb.f_em_natby_cat_co2e_category(
            input_pub_year_id, input_layer_id, input_category_name, input_ghg_category_name
        ) prev_cat
    ON current_cat.year = prev_cat.year;

END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_sector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_sector(input_pub_year_id integer, input_layer_id integer, input_sector_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
begin

    -- Return the difference between the current and previous year's total values
    RETURN QUERY
    SELECT
        current_cat.year,
       (current_cat.total_quantity - COALESCE(prev_cat.total_quantity, 0)) as total_quantity
    FROM
        ggds_invdb.f_em_nat_cat_co2e_sector(
            input_pub_year_id , input_layer_id , input_sector_name  , input_ghg_category_name 
        ) current_cat
    LEFT JOIN
        ggds_invdb.f_em_natby_cat_co2e_sector(
            input_pub_year_id , input_layer_id , input_sector_name  , input_ghg_category_name 
        ) prev_cat
    ON current_cat.year = prev_cat.year;

END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_subsector(int4, int4, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_subsector(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
begin

    -- Return the difference between the current and previous year's total values
    RETURN QUERY
    SELECT
        current_cat.year,
       (current_cat.total_quantity - COALESCE(prev_cat.total_quantity, 0)) as total_quantity
    FROM
        ggds_invdb.f_em_nat_cat_co2e_subsector(
            input_pub_year_id, input_layer_id, input_subsector_name, input_ghg_category_name
        ) current_cat
    LEFT JOIN
        ggds_invdb.f_em_natby_cat_co2e_subsector(
            input_pub_year_id, input_layer_id, input_subsector_name, input_ghg_category_name
        ) prev_cat
    ON current_cat.year = prev_cat.year;

END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_subsector_category(int4, int4, text, text, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_natbydiff_cat_co2e_subsector_category(input_pub_year_id integer, input_layer_id integer, input_subsector_name text, input_category_name text, input_ghg_category_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
begin

    -- Return the difference between the current and previous year's total values
    RETURN QUERY
    SELECT
        current_cat.year,
       (current_cat.total_quantity - COALESCE(prev_cat.total_quantity, 0)) as total_quantity
    FROM
        ggds_invdb.f_em_nat_cat_co2e_subsector_category(
            input_pub_year_id , input_layer_id , input_subsector_name , input_category_name , input_ghg_category_name 
        ) current_cat
    LEFT JOIN
        ggds_invdb.f_em_natby_cat_co2e_subsector_category(
            input_pub_year_id , input_layer_id , input_subsector_name , input_category_name , input_ghg_category_name 
        ) prev_cat
    ON current_cat.year = prev_cat.year;

END
$function$
;


-- DROP FUNCTION ggds_invdb.f_em_nat_natbydiff_co2e_subsector_all(int4, int4, text);

CREATE OR REPLACE FUNCTION ggds_invdb.f_em_nat_natbydiff_co2e_subsector_all(input_pub_year_id integer, input_layer_id integer, input_subsector_name text)
 RETURNS TABLE(year integer, total_quantity numeric)
 LANGUAGE plpgsql
AS $function$
begin

    -- Return the difference between the current and previous year's total values
    RETURN QUERY
    SELECT
        current_cat.year,
       (current_cat.total_quantity - COALESCE(prev_cat.total_quantity, 0)) as total_quantity
    FROM
        ggds_invdb.f_em_nat_co2e_subsector_all(
            input_pub_year_id , input_layer_id , input_subsector_name 
        ) current_cat
    LEFT JOIN
        ggds_invdb.f_em_natby_co2e_subsector_all(
            input_pub_year_id , input_layer_id , input_subsector_name 
        ) prev_cat
    ON current_cat.year = prev_cat.year;

END
$function$
;
