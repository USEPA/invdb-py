/*========================================================================================
The following query functions are used to produce state publication data products.
========================================================================================*/ 


CREATE OR REPLACE FUNCTION ggds_invdb.EM_STA_ALL_SNAPSHOT(input_pub_year_id INTEGER, input_layer_id INTEGER) RETURNS TABLE(data_key TEXT, sector INTEGER, subsector INTEGER, category INTEGER, sub_category_1 TEXT, 
                                                                                                                           sub_category_2 TEXT, sub_category_3 TEXT, sub_category_4 TEXT, sub_category_5 TEXT, 
                                                                                                                           carbon_pool TEXT, fuel1 INTEGER, fuel2 INTEGER, GHG INTEGER, ghg_category_name TEXT, geo_ref TEXT,
                                                                                                                           exclude TEXT, crt_code TEXT, id TEXT, cbi_activity TEXT, units TEXT,  
                                                                                                                           year INTEGER, weighted_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var TEXT;
	max_year_id_var INTEGER;
    results_record RECORD;
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
	DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_temp_info;
	EXECUTE format('CREATE TABLE ggds_invdb.em_sta_all_snapshot_temp_info AS
					SELECT ghg.ghg_id, ghg_cat.ghg_category_name, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
                    JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg_cat.ghg_category_id = ghg.ghg_category_id',
					gwp_version_var);

    -- draw all the data from the emissions_key and facts_archive tables. 
    -- convert text values to zeroes, multiply by GWP, and translate IDs to actual names

    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_0_from_prepare_script;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_0_from_prepare_script AS
    SELECT
        NULL::TEXT as data_key,
        em.sector_id::INTEGER AS sector,
        em.sub_sector_id::INTEGER AS subsector,
        em.category_id::INTEGER AS category,
        em.sub_category_1::TEXT,
        em.sub_category_2::TEXT,
        em.sub_category_3::TEXT,
        em.sub_category_4::TEXT,
        em.sub_category_5::TEXT,
        em.carbon_pool::TEXT,
        em.fuel_type_id_1::INTEGER AS fuel1,
        em.fuel_type_id_2::INTEGER AS fuel2,
        em.ghg_id::INTEGER as GHG,
        temp.ghg_category_name::TEXT as GHG_category,
        em.geo_ref::TEXT,
        em."EXCLUDE"::TEXT AS exclude,
        em.crt_code::TEXT,
        em.id::TEXT,
        em.cbi_activity::TEXT,
        em.units::TEXT,
        fa.year_id::INTEGER,
        (CASE 
            WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric
            ELSE 0 
            END * temp.gwp_factor
        ) AS weighted_quantity
    FROM ggds_invdb.emissions_key em 
        JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
        LEFT JOIN ggds_invdb.em_sta_all_snapshot_temp_info temp ON em.ghg_id = temp.ghg_id
    WHERE fa.year_id <= max_year_id_var
        AND fa.layer_id = input_layer_id
        AND fa.pub_year_id = input_pub_year_id
    ORDER BY fa.key_id, fa.year_id ASC;

    RETURN QUERY
    SELECT * 
    FROM ggds_invdb.em_sta_all_snapshot_0_from_prepare_script;

    --DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_temp_info;
    
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.EM_STA_ALL_SNAPSHOT(11, 1); 


--=====================================================================================================================================
--=====================================================================================================================================


CREATE OR REPLACE FUNCTION ggds_invdb.EM_STA_ALL_REFINED(input_pub_object_id INTEGER) RETURNS JSON AS--TABLE(data_key TEXT, sector TEXT, subsector TEXT, category TEXT, sub_category_1 TEXT, 
--                                                                                                     sub_category_2 TEXT, sub_category_3 TEXT, sub_category_4 TEXT, sub_category_5 TEXT, 
--                                                                                                     carbon_pool TEXT, fuel1 TEXT, fuel2 TEXT, geo_ref TEXT, exclude TEXT, 
--                                                                                                     crt_code TEXT, id TEXT, cbi_activity TEXT, units TEXT, GHG TEXT, ghg_category TEXT,
--                                                                                                     Y1990 NUMERIC,Y1991 NUMERIC,Y1992 NUMERIC,Y1993 NUMERIC,Y1994 NUMERIC,
--                                                                                                     Y1995 NUMERIC,Y1996 NUMERIC,Y1997 NUMERIC,Y1998 NUMERIC,Y1999 NUMERIC,
--                                                                                                     Y2000 NUMERIC,Y2001 NUMERIC,Y2002 NUMERIC,Y2003 NUMERIC,Y2004 NUMERIC,
--                                                                                                     Y2005 NUMERIC,Y2006 NUMERIC,Y2007 NUMERIC,Y2008 NUMERIC,Y2009 NUMERIC,
--                                                                                                     Y2010 NUMERIC,Y2011 NUMERIC,Y2012 NUMERIC,Y2013 NUMERIC,Y2014 NUMERIC,
--                                                                                                     Y2015 NUMERIC,Y2016 NUMERIC,Y2017 NUMERIC,Y2018 NUMERIC,Y2019 NUMERIC,
--                                                                                                     Y2020 NUMERIC,Y2021 NUMERIC,Y2022 NUMERIC) AS 
$BODY$
declare
    raw_data_JSON JSONB;
    refined_data_JSON JSON;
    row_count INT;
    row_count2 INT;
begin
	-- pull the data from the raw_data cell
    SELECT raw_data::JSONB INTO raw_data_json
    FROM ggds_invdb.publication_object po
    WHERE po.pub_object_id = input_pub_object_id;

    -- convert it to a table
    DROP TABLE IF EXISTS em_sta_all_refined_interim;
    CREATE TEMPORARY TABLE em_sta_all_refined_interim(
        data_key TEXT,
        sector TEXT,
        subsector TEXT,
        category TEXT,
        sub_category_1 TEXT,
        sub_category_2 TEXT,
        sub_category_3 TEXT,
        sub_category_4 TEXT,
        sub_category_5 TEXT,
        carbon_pool TEXT,
        fuel1 TEXT,
        fuel2 TEXT,
        GHG TEXT,
        ghg_category TEXT,
        geo_ref TEXT,
        exclude TEXT,
        crt_code TEXT,
        id TEXT,
        cbi_activity TEXT,
        units TEXT,
        "Y1990" NUMERIC,
        "Y1991" NUMERIC,
        "Y1992" NUMERIC,
        "Y1993" NUMERIC,
        "Y1994" NUMERIC,
        "Y1995" NUMERIC,
        "Y1996" NUMERIC,
        "Y1997" NUMERIC,
        "Y1998" NUMERIC,
        "Y1999" NUMERIC,
        "Y2000" NUMERIC,
        "Y2001" NUMERIC,
        "Y2002" NUMERIC,
        "Y2003" NUMERIC,
        "Y2004" NUMERIC,
        "Y2005" NUMERIC,
        "Y2006" NUMERIC,
        "Y2007" NUMERIC,
        "Y2008" NUMERIC,
        "Y2009" NUMERIC,
        "Y2010" NUMERIC,
        "Y2011" NUMERIC,
        "Y2012" NUMERIC,
        "Y2013" NUMERIC,
        "Y2014" NUMERIC,
        "Y2015" NUMERIC,
        "Y2016" NUMERIC,
        "Y2017" NUMERIC,
        "Y2018" NUMERIC,
        "Y2019" NUMERIC,
        "Y2020" NUMERIC,
        "Y2021" NUMERIC,
        "Y2022" NUMERIC
    );
    
    INSERT INTO em_sta_all_refined_interim
    SELECT
        CASE WHEN j.value->'data_key' = 'null' THEN NULL        ELSE REPLACE((j.value->'data_key')::TEXT, '"', '') END,
        CASE WHEN j.value->'sector' = 'null' THEN NULL          ELSE REPLACE((j.value->'sector')::TEXT, '"', '') END,
        CASE WHEN j.value->'subsector' = 'null' THEN NULL       ELSE REPLACE((j.value->'subsector')::TEXT, '"', '') END,
        CASE WHEN j.value->'category' = 'null' THEN NULL        ELSE REPLACE((j.value->'category')::TEXT, '"', '') END,
        CASE WHEN j.value->'sub_category_1' = 'null' THEN NULL  ELSE REPLACE((j.value->'sub_category_1')::TEXT, '"', '') END,
        CASE WHEN j.value->'sub_category_2' = 'null' THEN NULL  ELSE REPLACE((j.value->'sub_category_2')::TEXT, '"', '') END,
        CASE WHEN j.value->'sub_category_3' = 'null' THEN NULL  ELSE REPLACE((j.value->'sub_category_3')::TEXT, '"', '') END,
        CASE WHEN j.value->'sub_category_4' = 'null' THEN NULL  ELSE REPLACE((j.value->'sub_category_4')::TEXT, '"', '') END,
        CASE WHEN j.value->'sub_category_5' = 'null' THEN NULL  ELSE REPLACE((j.value->'sub_category_5')::TEXT, '"', '') END,
        CASE WHEN j.value->'carbon_pool' = 'null' THEN NULL     ELSE REPLACE((j.value->'carbon_pool')::TEXT, '"', '') END,
        CASE WHEN j.value->'fuel1' = 'null' THEN NULL           ELSE REPLACE((j.value->'fuel1')::TEXT, '"', '') END,
        CASE WHEN j.value->'fuel2' = 'null' THEN NULL           ELSE REPLACE((j.value->'fuel2')::TEXT, '"', '') END,
        CASE WHEN j.value->'ghg' = 'null' THEN NULL             ELSE REPLACE((j.value->'ghg')::TEXT, '"', '') END,
        CASE WHEN j.value->'ghg_category' = 'null' THEN NULL    ELSE REPLACE((j.value->'ghg_category')::TEXT, '"', '') END,
        CASE WHEN j.value->'geo_ref' = 'null' THEN NULL         ELSE REPLACE((j.value->'geo_ref')::TEXT, '"', '') END,
        CASE WHEN j.value->'exclude' = 'null' THEN NULL         ELSE REPLACE((j.value->'exclude')::TEXT, '"', '') END,
        CASE WHEN j.value->'crt_code' = 'null' THEN NULL        ELSE REPLACE((j.value->'crt_code')::TEXT, '"', '') END,
        CASE WHEN j.value->'id' = 'null' THEN NULL              ELSE REPLACE((j.value->'id')::TEXT, '"', '') END,
        CASE WHEN j.value->'cbi_activity' = 'null' THEN NULL    ELSE REPLACE((j.value->'cbi_activity')::TEXT, '"', '') END,
        CASE WHEN j.value->'units' = 'null' THEN NULL           ELSE REPLACE((j.value->'units')::TEXT, '"', '') END,
        (j.value->'Y1990')::NUMERIC,
        (j.value->'Y1991')::NUMERIC,
        (j.value->'Y1992')::NUMERIC,
        (j.value->'Y1993')::NUMERIC,
        (j.value->'Y1994')::NUMERIC,
        (j.value->'Y1995')::NUMERIC,
        (j.value->'Y1996')::NUMERIC,
        (j.value->'Y1997')::NUMERIC,
        (j.value->'Y1998')::NUMERIC,
        (j.value->'Y1999')::NUMERIC,
        (j.value->'Y2000')::NUMERIC,
        (j.value->'Y2001')::NUMERIC,
        (j.value->'Y2002')::NUMERIC,
        (j.value->'Y2003')::NUMERIC,
        (j.value->'Y2004')::NUMERIC,
        (j.value->'Y2005')::NUMERIC,
        (j.value->'Y2006')::NUMERIC,
        (j.value->'Y2007')::NUMERIC,
        (j.value->'Y2008')::NUMERIC,
        (j.value->'Y2009')::NUMERIC,
        (j.value->'Y2010')::NUMERIC,
        (j.value->'Y2011')::NUMERIC,
        (j.value->'Y2012')::NUMERIC,
        (j.value->'Y2013')::NUMERIC,
        (j.value->'Y2014')::NUMERIC,
        (j.value->'Y2015')::NUMERIC,
        (j.value->'Y2016')::NUMERIC,
        (j.value->'Y2017')::NUMERIC,
        (j.value->'Y2018')::NUMERIC,
        (j.value->'Y2019')::NUMERIC,
        (j.value->'Y2020')::NUMERIC,
        (j.value->'Y2021')::NUMERIC,
        (j.value->'Y2022')::NUMERIC
    FROM jsonb_array_elements(raw_data_JSON) AS j(value);

    SELECT COUNT(*) INTO row_count FROM em_sta_all_refined_interim;
    RAISE NOTICE 'fetched % rows from the raw_data.', row_count;

    -- post the before results
    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_1_before_redaction;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_1_before_redaction
    AS SELECT * FROM em_sta_all_refined_interim;



    -- perform the redaction
    -- 4) redact chemicals according to the dim_redacted_ghg chemicals
    DROP TABLE IF EXISTS em_sta_all_snapshot_redacted_chemicals;
    CREATE TEMPORARY TABLE em_sta_all_snapshot_redacted_chemicals AS (
        SELECT 
            t.sector,
            t.subsector,
            t.category,
            t.sub_category_1,
            t.sub_category_2,
            t.sub_category_3,
            t.sub_category_4,
            t.sub_category_5,
            t.carbon_pool,
            t.fuel1,
            t.fuel2,
            redacted_ghgs.redacted_ghg_name as ghg,
            t.ghg_category,
            t.geo_ref,
            t.exclude,
            t.crt_code,
            t.id,
            t.cbi_activity,
            t.units,
            SUM(t."Y1990") AS "Y1990",
            SUM(t."Y1991") AS "Y1991",
            SUM(t."Y1992") AS "Y1992",
            SUM(t."Y1993") AS "Y1993",
            SUM(t."Y1994") AS "Y1994",
            SUM(t."Y1995") AS "Y1995",
            SUM(t."Y1996") AS "Y1996",
            SUM(t."Y1997") AS "Y1997",
            SUM(t."Y1998") AS "Y1998",
            SUM(t."Y1999") AS "Y1999",
            SUM(t."Y2000") AS "Y2000",
            SUM(t."Y2001") AS "Y2001",
            SUM(t."Y2002") AS "Y2002",
            SUM(t."Y2003") AS "Y2003",
            SUM(t."Y2004") AS "Y2004",
            SUM(t."Y2005") AS "Y2005",
            SUM(t."Y2006") AS "Y2006",
            SUM(t."Y2007") AS "Y2007",
            SUM(t."Y2008") AS "Y2008",
            SUM(t."Y2009") AS "Y2009",
            SUM(t."Y2010") AS "Y2010",
            SUM(t."Y2011") AS "Y2011",
            SUM(t."Y2012") AS "Y2012",
            SUM(t."Y2013") AS "Y2013",
            SUM(t."Y2014") AS "Y2014",
            SUM(t."Y2015") AS "Y2015",
            SUM(t."Y2016") AS "Y2016",
            SUM(t."Y2017") AS "Y2017",
            SUM(t."Y2018") AS "Y2018",
            SUM(t."Y2019") AS "Y2019",
            SUM(t."Y2020") AS "Y2020",
            SUM(t."Y2021") AS "Y2021",
            SUM(t."Y2022") AS "Y2022"
        FROM (SELECT DISTINCT * FROM em_sta_all_refined_interim) t
            LEFT JOIN ggds_invdb.dim_ghg ghgs ON ghgs.ghg_longname = t.ghg
            JOIN ggds_invdb.dim_redacted_ghg redacted_ghgs ON ghgs.ghg_id = redacted_ghgs.ghg_id 
        WHERE t.category = 'Substitution of Ozone Depleting Substances'
        GROUP BY t.sector, t.subsector,
                t.category, t.sub_category_1, t.sub_category_2,
                t.sub_category_3, t.sub_category_4, t.sub_category_5,
                t.carbon_pool, t.fuel1, t.fuel2, t.geo_ref, t.exclude,
                t.crt_code, t.id, t.cbi_activity, t.units, redacted_ghgs.redacted_ghg_name, 
                t.ghg_category
        ORDER BY t.sub_category_1, t.geo_ref
    ); 

    SELECT COUNT(*) INTO row_count FROM em_sta_all_snapshot_redacted_chemicals;
    RAISE NOTICE 'found % aggregated redacted chemical rows.', row_count;


    -- post the before results
    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_2_redacted_row_groups;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_2_redacted_row_groups
    AS SELECT * FROM em_sta_all_snapshot_redacted_chemicals;

    SELECT COUNT(*) INTO row_count FROM em_sta_all_refined_interim;

    -- delete the redacted individual rows from the temp table (this works, just make sure dim_redacted_ghg is up-to-date!)
    DELETE FROM em_sta_all_refined_interim t
    WHERE EXISTS (
        SELECT dim_ghg.ghg_longname as ghg, redacted_ghgs.category as category
        FROM     ggds_invdb.dim_redacted_ghg redacted_ghgs
            JOIN ggds_invdb.dim_ghg dim_ghg ON dim_ghg.ghg_id = redacted_ghgs.ghg_id
        WHERE t.ghg = dim_ghg.ghg_longname
                AND t.category = redacted_ghgs.category 
    );

    SELECT COUNT(*) INTO row_count2 FROM em_sta_all_refined_interim;
    RAISE NOTICE 'removed % UNaggregated redacted chemical rows from the main table.', 
                 row_count - row_count2;

    
    SELECT COUNT(*) INTO row_count FROM em_sta_all_refined_interim;

    -- insert the redacted aggregations into the temp table
    INSERT INTO em_sta_all_refined_interim
        SELECT NULL, * 
        FROM em_sta_all_snapshot_redacted_chemicals;

    SELECT COUNT(*) INTO row_count2 FROM em_sta_all_refined_interim;
    RAISE NOTICE 'added % aggregated redacted chemical rows to the main table.', 
                 row_count2 - row_count;
    
    -- post the after results
    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_3_after_redaction;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_3_after_redaction
    AS SELECT * FROM em_sta_all_refined_interim;

    -- readd the data_key hashes to the redacted chemical rows
    UPDATE em_sta_all_refined_interim
    SET data_key = MD5(
            format(
                '[''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'', ''%s'']', 
                sector,
                subsector,
                category,
                sub_category_1,
                sub_category_2,
                sub_category_3,
                sub_category_4,
                sub_category_5,
                carbon_pool,
                fuel1,
                fuel2,
                ghg,
                ghg_category,
                exclude,
                crt_code,
                id,
                cbi_activity,
                units)
            )
    WHERE data_key IS NULL;
    
    -- post the after results
    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_4_before_blanks_padding;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_4_before_blanks_padding
    AS SELECT * FROM em_sta_all_refined_interim;

    -- set any blank year values to 0
    UPDATE em_sta_all_refined_interim t
    SET "Y1990" = CASE WHEN t."Y1990" IS NULL THEN 0 ELSE t."Y1990" END,
        "Y1991" = CASE WHEN t."Y1991" IS NULL THEN 0 ELSE t."Y1991" END,
        "Y1992" = CASE WHEN t."Y1992" IS NULL THEN 0 ELSE t."Y1992" END,
        "Y1993" = CASE WHEN t."Y1993" IS NULL THEN 0 ELSE t."Y1993" END,
        "Y1994" = CASE WHEN t."Y1994" IS NULL THEN 0 ELSE t."Y1994" END,
        "Y1995" = CASE WHEN t."Y1995" IS NULL THEN 0 ELSE t."Y1995" END,
        "Y1996" = CASE WHEN t."Y1996" IS NULL THEN 0 ELSE t."Y1996" END,
        "Y1997" = CASE WHEN t."Y1997" IS NULL THEN 0 ELSE t."Y1997" END,
        "Y1998" = CASE WHEN t."Y1998" IS NULL THEN 0 ELSE t."Y1998" END,
        "Y1999" = CASE WHEN t."Y1999" IS NULL THEN 0 ELSE t."Y1999" END,
        "Y2000" = CASE WHEN t."Y2000" IS NULL THEN 0 ELSE t."Y2000" END,
        "Y2001" = CASE WHEN t."Y2001" IS NULL THEN 0 ELSE t."Y2001" END,
        "Y2002" = CASE WHEN t."Y2002" IS NULL THEN 0 ELSE t."Y2002" END,
        "Y2003" = CASE WHEN t."Y2003" IS NULL THEN 0 ELSE t."Y2003" END,
        "Y2004" = CASE WHEN t."Y2004" IS NULL THEN 0 ELSE t."Y2004" END,
        "Y2005" = CASE WHEN t."Y2005" IS NULL THEN 0 ELSE t."Y2005" END,
        "Y2006" = CASE WHEN t."Y2006" IS NULL THEN 0 ELSE t."Y2006" END,
        "Y2007" = CASE WHEN t."Y2007" IS NULL THEN 0 ELSE t."Y2007" END,
        "Y2008" = CASE WHEN t."Y2008" IS NULL THEN 0 ELSE t."Y2008" END,
        "Y2009" = CASE WHEN t."Y2009" IS NULL THEN 0 ELSE t."Y2009" END,
        "Y2010" = CASE WHEN t."Y2010" IS NULL THEN 0 ELSE t."Y2010" END,
        "Y2011" = CASE WHEN t."Y2011" IS NULL THEN 0 ELSE t."Y2011" END,
        "Y2012" = CASE WHEN t."Y2012" IS NULL THEN 0 ELSE t."Y2012" END,
        "Y2013" = CASE WHEN t."Y2013" IS NULL THEN 0 ELSE t."Y2013" END,
        "Y2014" = CASE WHEN t."Y2014" IS NULL THEN 0 ELSE t."Y2014" END,
        "Y2015" = CASE WHEN t."Y2015" IS NULL THEN 0 ELSE t."Y2015" END,
        "Y2016" = CASE WHEN t."Y2016" IS NULL THEN 0 ELSE t."Y2016" END,
        "Y2017" = CASE WHEN t."Y2017" IS NULL THEN 0 ELSE t."Y2017" END,
        "Y2018" = CASE WHEN t."Y2018" IS NULL THEN 0 ELSE t."Y2018" END,
        "Y2019" = CASE WHEN t."Y2019" IS NULL THEN 0 ELSE t."Y2019" END,
        "Y2020" = CASE WHEN t."Y2020" IS NULL THEN 0 ELSE t."Y2020" END,
        "Y2021" = CASE WHEN t."Y2021" IS NULL THEN 0 ELSE t."Y2021" END,
        "Y2022" = CASE WHEN t."Y2022" IS NULL THEN 0 ELSE t."Y2022" END;

    -- post the after results
    DROP TABLE IF EXISTS ggds_invdb.em_sta_all_snapshot_5_after_blanks_padding;
    CREATE TABLE ggds_invdb.em_sta_all_snapshot_5_after_blanks_padding
    AS SELECT * FROM em_sta_all_refined_interim;

    -- return the after results
    -- RETURN QUERY
    -- SELECT *
    -- FROM em_sta_all_refined_interim;

    EXECUTE 'SELECT json_agg(landscape_data)
    FROM (
    SELECT *
    FROM em_sta_all_refined_interim
    ) landscape_data'
    INTO refined_data_JSON;

    RETURN refined_data_JSON;

    DROP TABLE IF EXISTS em_sta_all_refined_interim;
    DROP TABLE IF EXISTS em_sta_all_snapshot_redacted_chemicals;
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
select *
from ggds_invdb.EM_STA_ALL_REFINED(104); 


--=====================================================================================================================================
--=====================================================================================================================================


CREATE OR REPLACE FUNCTION ggds_invdb.EM_STA_SECTOR_SNAPSHOT(input_pub_year_id INTEGER, input_layer_id INTEGER) RETURNS TABLE(sector INTEGER, ghg_category_name TEXT, year INTEGER, weighted_quantity NUMERIC) AS 
$BODY$
declare
	gwp_version_var TEXT;
	max_year_id_var INTEGER;
    results_record RECORD;
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
	DROP TABLE IF EXISTS em_sta_sector_snapshot_temp_info;
	EXECUTE format('CREATE TEMPORARY TABLE em_sta_sector_snapshot_temp_info AS
					SELECT ghg_cat.ghg_category_name, ghg.ghg_id, ghg.%s as gwp_factor
					FROM ggds_invdb.dim_ghg ghg
                        JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg_cat.ghg_category_id = ghg.ghg_category_id
                    WHERE ghg_cat.ghg_category_name != ''HFE'' ', -- exclude HFEs from the results
					gwp_version_var);

    -- draw all the data from the emissions_key and facts_archive tables. 
    -- convert text values to zeroes, multiply by GWP, and translate IDs to actual names
    RETURN QUERY
        SELECT 
            em.sector_id::INTEGER AS sector,
            ghg.ghg_category_name::TEXT,
            fa.year_id::INTEGER,
            SUM(CASE 
                    WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric
                    ELSE 0 END * ghg.gwp_factor
            ) AS weighted_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN em_sta_sector_snapshot_temp_info ghg ON em.ghg_id = ghg.ghg_id
        WHERE fa.year_id <= max_year_id_var
            AND fa.layer_id = input_layer_id
            AND fa.pub_year_id = input_pub_year_id
        GROUP BY 
            em.sector_id,
            ghg.ghg_category_name,
            fa.year_id
        ORDER BY 
            em.sector_id,
            ghg.ghg_category_name,
            fa.year_id ASC;

    DROP TABLE IF EXISTS em_sta_sector_snapshot_temp_info;
    
END
$BODY$ LANGUAGE PLPGSQL;

-- test with results
-- select *
-- from ggds_invdb.EM_STA_SECTOR_SNAPSHOT(11, 1);



CREATE OR REPLACE FUNCTION ggds_invdb.ACT_STA_POPGDP(input_pub_year_id INTEGER, input_layer_id INTEGER) RETURNS JSON AS 
$BODY$
declare
    gwp_version_var TEXT;
    max_year_id_var INTEGER;
    results_record RECORD;
    time_series_column_list_str TEXT;
    time_series_column_select_list_str TEXT;
    max_time_series_var INTEGER;
    sql_statement TEXT;
    return_json JSON;
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
    DROP TABLE IF EXISTS act_sta_popgdp_snapshot_temp_info;
    EXECUTE format('CREATE TEMPORARY TABLE act_sta_popgdp_snapshot_temp_info AS
                    SELECT ghg.ghg_id, ghg.%s as gwp_factor
                    FROM ggds_invdb.dim_ghg ghg', -- exclude HFEs from the results
                    gwp_version_var);

    -- draw all the data from the emissions_key and facts_archive tables. 
    -- convert text values to zeroes, multiply by GWP
    DROP TABLE IF EXISTS act_sta_popgdp_portrait_temp_data;
    CREATE TEMPORARY TABLE act_sta_popgdp_portrait_temp_data AS
        SELECT 
            em.emissions_uid::TEXT as key_id,
            em.sub_category_1 as sub_category_1,
            em.geo_ref as geo_ref, 
            years.year as year,
            SUM(CASE 
                    WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN fa.value::numeric
                    ELSE 0 END * ghg.gwp_factor
            ) AS weighted_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN act_sta_popgdp_snapshot_temp_info ghg ON em.ghg_id = ghg.ghg_id
            JOIN ggds_invdb.DIM_TIME_SERIES years ON fa.year_id = years.year_id
        WHERE fa.year_id <= max_year_id_var
            AND fa.layer_id = input_layer_id
            AND fa.pub_year_id = input_pub_year_id
            AND em.sub_category_1 IN ('population', 'GDP')
        GROUP BY 
            em.emissions_uid,
            em.sub_category_1,
            em.geo_ref,
            years.year
        ORDER BY 
            em.emissions_uid,
            em.sub_category_1,
            em.geo_ref,
            years.year ASC;

    -- send the portrait data to a landscape table

    -- fetch the max time series
    SELECT py.max_time_series INTO max_time_series_var
    FROM ggds_invdb.dim_publication_year py
    WHERE pub_year_id = input_pub_year_id;

    -- generate the time series column list string
    time_series_column_list_str := '';
    time_series_column_select_list_str := '';
    FOR year_value IN 1990..max_time_series_var
    LOOP
        time_series_column_list_str = time_series_column_list_str || format('"Y%s" NUMERIC, ', year_value);
        time_series_column_select_list_str = time_series_column_select_list_str || format('"Y%s", ', year_value);
    END LOOP;
    time_series_column_list_str := substr(time_series_column_list_str, 1, length(time_series_column_list_str) - 2);
    time_series_column_select_list_str := substr(time_series_column_select_list_str, 1, length(time_series_column_select_list_str) - 2);

    -- form a table with the desired time series quantities related to their keys
    DROP TABLE IF EXISTS ggds_invdb.act_sta_popgdp_temp_landscape_imm;
    sql_statement := '
        CREATE TABLE ggds_invdb.act_sta_popgdp_temp_landscape_imm AS
        SELECT *
        FROM crosstab(
            ''SELECT key_id::TEXT, year, weighted_quantity FROM act_sta_popgdp_portrait_temp_data ORDER BY key_id, year'',
            ''SELECT DISTINCT year FROM act_sta_popgdp_portrait_temp_data ORDER BY year''
        ) AS ct (
            key_id TEXT, ' || time_series_column_list_str || '
        );';
    EXECUTE sql_statement;

    -- join the table above with the other columns by the keys
    DROP TABLE IF EXISTS ggds_invdb.act_sta_popgdp_temp_landscape;
    sql_statement = '
    CREATE TABLE ggds_invdb.act_sta_popgdp_temp_landscape AS (
    SELECT DISTINCT
            info.key_id::TEXT as key_id,
            info.sub_category_1,
            info.geo_ref,
            ' || time_series_column_select_list_str || '
            FROM act_sta_popgdp_portrait_temp_data info 
            JOIN ggds_invdb.act_sta_popgdp_temp_landscape_imm imm ON info.key_id::TEXT = imm.key_id::TEXT
    );';
    EXECUTE sql_statement;

    -- Return the landscape data as a JSON
    EXECUTE 'SELECT json_agg(landscape_data)
    FROM (
    SELECT *
    FROM ggds_invdb.act_sta_popgdp_temp_landscape
    ) landscape_data'
    INTO return_json;

    RETURN return_json;

    DROP TABLE IF EXISTS act_sta_popgdp_portrait_temp_data;
    DROP TABLE IF EXISTS ggds_invdb.act_sta_popgdp_temp_landscape_imm;
    DROP TABLE IF EXISTS ggds_invdb.act_sta_popgdp_temp_landscape;
    
END
$BODY$ LANGUAGE PLPGSQL;

--test with results
-- select *
-- from ggds_invdb.ACT_STA_POPGDP(11, 2);
