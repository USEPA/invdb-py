CREATE OR REPLACE FUNCTION ggds_invdb.em_all_snapshot_with_annotations(input_pub_year_id integer, input_layer_id integer)
RETURNS TABLE(key_id text, sector text, subsector text, category text, sub_category_1 text, sub_category_2 text, sub_category_3 text, sub_category_4 text, sub_category_5 text, carbon_pool text, fuel1 text, fuel2 text, geo_ref text, exclude text, crt_code text, id text, cbi_activity text, units text, ghg text, gwp numeric, ghg_category text, year integer, weighted_quantity text)
AS $BODY$
    DECLARE
        gwp_version_var TEXT;
        max_year_id_var INTEGER;
        results_record RECORD;
    BEGIN
        -- select the GWP version and max time series year ID based on the input publication year ID
        WITH pub_year_info AS (
            SELECT LOWER(gwp_column) AS gwp_version, max_time_series 
            FROM ggds_invdb.dim_publication_year
            WHERE pub_year_id = input_pub_year_id
        )
        SELECT pub_year_info.gwp_version, dts.year_id INTO gwp_version_var, max_year_id_var
        FROM ggds_invdb.dim_time_series dts
            JOIN pub_year_info on pub_year_info.max_time_series = dts.year;
            
        -- select the GHG IDs and their GWP factors based on the selected GWP version from the above query
        DROP TABLE IF EXISTS em_all_snapshot_with_annotations_temp_info;
        EXECUTE format('CREATE TEMPORARY TABLE em_all_snapshot_with_annotations_temp_info AS
                        SELECT ghg.ghg_id, ghg.%s as gwp_factor
                        FROM ggds_invdb.dim_ghg ghg',
                        gwp_version_var);

        -- draw all the data from the emissions_key and facts_archive tables. 
        -- convert text values to zeroes, multiply by GWP, and translate IDs to actual names
        RETURN QUERY
        SELECT 
            fa.key_id::TEXT,
            sectors.sector_name::TEXT AS sector,
            subsectors.subsector_name::TEXT AS subsector,
            categories.category_name::TEXT AS category,
            em.sub_category_1::TEXT,
            em.sub_category_2::TEXT,
            em.sub_category_3::TEXT,
            em.sub_category_4::TEXT,
            em.sub_category_5::TEXT,
            em.carbon_pool::TEXT,
            fuel1_types.fuel_type_name::TEXT AS fuel1,
            fuel2_types.fuel_type_name::TEXT AS fuel2,
            em.geo_ref::TEXT,
            em."EXCLUDE"::TEXT AS exclude,
            em.crt_code::TEXT,
            em.id::TEXT,
            em.cbi_activity::TEXT,
            em.units::TEXT,
            ghgs.ghg_longname::TEXT as ghg,
            em_all_snapshot_with_annotations_temp_info.gwp_factor::NUMERIC as gwp,
            ghg_cat.ghg_category_name::TEXT as ghg_category, -- Added by INVDB-503
            years.year::INTEGER,
            (CASE 
                WHEN fa.value ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN TRIM((fa.value::numeric * em_all_snapshot_with_annotations_temp_info.gwp_factor)::TEXT)
                ELSE TRIM(fa.value::TEXT)
            END 
            ) AS weighted_quantity
        FROM ggds_invdb.emissions_key em 
            JOIN ggds_invdb.facts_archive fa ON em.emissions_uid::text = fa.key_id::text
            JOIN em_all_snapshot_with_annotations_temp_info ON em.ghg_id = em_all_snapshot_with_annotations_temp_info.ghg_id
            LEFT JOIN ggds_invdb.dim_sector sectors ON sectors.sector_id = em.sector_id
            LEFT JOIN ggds_invdb.dim_subsector subsectors ON subsectors.subsector_id = em.sub_sector_id
            LEFT JOIN ggds_invdb.dim_category categories ON categories.category_id = em.category_id
            LEFT JOIN ggds_invdb.dim_fuel_type fuel1_types ON fuel1_types.fuel_type_id = em.fuel_type_id_1
            LEFT JOIN ggds_invdb.dim_fuel_type fuel2_types ON fuel2_types.fuel_type_id = em.fuel_type_id_2
            LEFT JOIN ggds_invdb.dim_ghg ghgs ON ghgs.ghg_id = em.ghg_id
            LEFT JOIN ggds_invdb.dim_ghg_category ghg_cat ON ghg_cat.ghg_category_id = ghgs.ghg_category_id
            LEFT JOIN ggds_invdb.DIM_TIME_SERIES years ON years.year_id = fa.year_id
        WHERE fa.year_id <= max_year_id_var
            AND fa.layer_id = input_layer_id
            AND ghg_cat.ghg_category_name NOT IN ('HFE', 'Other') -- Added by INVDB-503
        ORDER BY fa.key_id, fa.year_id ASC;

        DROP TABLE IF EXISTS em_all_snapshot_with_annotations_temp_info;
        
    END
    $BODY$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION ggds_invdb.em_sta_powerusers_snapshot(input_pub_object_id integer, input_pub_year_id integer, layer_id integer)
RETURNS void
LANGUAGE plpgsql
AS $function$
    DECLARE
        current_key_id TEXT;
        current_record ggds_invdb.PU_EMISSION_RECORD;
        current_array_record ggds_invdb.PU_EMISSION_RECORD;
        current_key_has_annotation BOOLEAN;
        current_rows_this_key ggds_invdb.PU_EMISSION_RECORD[];
        annotations_list TEXT[];
        time_series_column_list_strs TEXT[];
        time_series_str_count INTEGER;
        max_time_series INTEGER;
        sql_statement TEXT;
        i INTEGER;
        row_count INTEGER;
        return_json JSON;
    BEGIN
        annotations_list := ARRAY['NE', 'IE', 'C', 'NA', 'NO'];

        -- 2a) Create base dataset as in em_sta_all_snapshot but leave all values as is
        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_temp_portrait;
        CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_temp_portrait AS
            (SELECT snap.key_id, snap.sector, snap.subsector, 
                snap.category, snap.sub_category_1, 
                snap.sub_category_2, snap.sub_category_3, snap.sub_category_4, snap.sub_category_5, 
                snap.carbon_pool, snap.fuel1, snap.fuel2, snap.geo_ref, snap.exclude, 
                snap.crt_code, snap.id, snap.cbi_activity, snap.units, snap.ghg, snap.gwp, snap.ghg_category, snap.year, snap.weighted_quantity
            FROM ggds_invdb.em_all_snapshot_with_annotations(input_pub_year_id, layer_id) snap
            ORDER BY snap.key_id, snap.year); 

        -- print the row count
        SELECT COUNT(*) INTO row_count FROM em_sta_powerusers_snapshot_temp_portrait;
        RAISE NOTICE 'The base data function fetched % portrait rows.', row_count;
        
        -- convert the data to landscape
        --========================= PERFORM THE DATA PIVOT =======================
        -- fetch the max time series
        SELECT py.max_time_series INTO max_time_series
        FROM ggds_invdb.dim_publication_year py
        WHERE pub_year_id = input_pub_year_id;

        -- generate the time series column list string
        time_series_str_count := 7; -- must be equal to the number of distinct time series strings set below.
        time_series_column_list_strs = (ARRAY(SELECT '' FROM generate_series(1, time_series_str_count)));

        FOR year_value IN 1990..max_time_series
        LOOP
            time_series_column_list_strs[1] = time_series_column_list_strs[1]  || format('"Y%s" TEXT, ', year_value);
            time_series_column_list_strs[2] = time_series_column_list_strs[2]  || format('"Y%s", ', year_value);
            time_series_column_list_strs[3] = time_series_column_list_strs[3]  || format('t."Y%s", ', year_value);
            time_series_column_list_strs[4] = time_series_column_list_strs[4]  || format('SUM(t."Y%s") as "Y%s", ', year_value, year_value);
            time_series_column_list_strs[5] = time_series_column_list_strs[5]  || format('(t."Y%s" * -1), ', year_value);
            time_series_column_list_strs[6] = time_series_column_list_strs[6]  || format('(t."Y%s" * 0.5), ', year_value);
            time_series_column_list_strs[7] = time_series_column_list_strs[7]  || format('ALTER COLUMN "Y%s" TYPE NUMERIC USING "Y%s"::NUMERIC, ', year_value, year_value);
        END LOOP;

        FOR i in 1..time_series_str_count
        LOOP
            time_series_column_list_strs[i] := substr(time_series_column_list_strs[i], 1, length(time_series_column_list_strs[i]) - 2);
        END LOOP;


        -- create immediate table used to pivot the raw data
        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_temp_imm;
        EXECUTE format($f$
            CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_temp_imm AS
            SELECT *
            FROM crosstab(
                'SELECT key_id::TEXT, year, weighted_quantity FROM em_sta_powerusers_snapshot_temp_portrait ORDER BY key_id, year',
                'SELECT DISTINCT year FROM em_sta_powerusers_snapshot_temp_portrait ORDER BY year'
            ) AS ct (
                key_id TEXT, %s
            );$f$,
            time_series_column_list_strs[1]);

        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_temp;
        EXECUTE format($f$
            CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_temp AS (
                SELECT DISTINCT
                    info.key_id,
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
                    info.geo_ref,
                    info.exclude,
                    info.crt_code,
                    info.id,
                    info.cbi_activity,
                    info.units,
                    info.ghg,
                    info.gwp,
                    info.ghg_category,
                    %s
                FROM em_sta_powerusers_snapshot_temp_portrait info 
                JOIN em_sta_powerusers_snapshot_temp_imm imm ON info.key_id::TEXT = imm.key_id::TEXT
            );$f$, 
            time_series_column_list_strs[2]);
        --========================= DATA PIVOT COMPLETE ==========================


        -- 2b) Add e_or_i Column
        ALTER TABLE em_sta_powerusers_snapshot_temp ADD COLUMN e_or_i VARCHAR(10);
        UPDATE em_sta_powerusers_snapshot_temp t SET e_or_i = 'Both';
        UPDATE em_sta_powerusers_snapshot_temp t SET e_or_i = 'Econ Sect' WHERE t.sector = 'External Source';
        UPDATE em_sta_powerusers_snapshot_temp t SET e_or_i = 'Industry'  WHERE t.category = 'Substitution of Ozone Depleting Substances'; -- fixed by INVDB-502
        UPDATE em_sta_powerusers_snapshot_temp t SET e_or_i = 'Industry'  WHERE t.sector = 'Energy-Excluded';

        -- 2c) Add a row number field and populate
        ALTER TABLE em_sta_powerusers_snapshot_temp ADD COLUMN rownumber INTEGER;
        UPDATE em_sta_powerusers_snapshot_temp SET rownumber = row_numbers.row_number
            FROM (
                SELECT t.key_id, row_number() OVER (ORDER BY t.key_id ASC) AS row_number
                FROM em_sta_powerusers_snapshot_temp t
            ) AS row_numbers
            WHERE em_sta_powerusers_snapshot_temp.key_id = row_numbers.key_id;

        -- 2d) Correct rows where sector = "External Source"
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Fossil Fuel Combustion',              category='Industry'    WHERE t.sub_category_1='Mobile Non-Highway Construction';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Fossil Fuel Combustion',              category='Industry'    WHERE t.sub_category_1='Mobile Non-Highway Other';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Agriculture',                          subsector='Fossil Fuel Combustion',              category='Industrial'  WHERE t.sub_category_1='AG Stationary';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Agriculture',                          subsector='Fossil Fuel Combustion',              category='Agriculture' WHERE t.sub_category_1='Mobile Agricultural Equipment';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Product uses as substitutes for ODS', category=NULL          WHERE t.sub_category_1='ODS Subst Residential';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Product uses as substitutes for ODS', category=NULL          WHERE t.sub_category_1='ODS Subst Transportation';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Product uses as substitutes for ODS', category=NULL          WHERE t.sub_category_1='ODS Subst Commercial';
        UPDATE em_sta_powerusers_snapshot_temp t SET sector='Industrial Processes and Product Use', subsector='Product uses as substitutes for ODS', category=NULL          WHERE t.sub_category_1='ODS Subst Industry';

        -- 3) Create annotations table that holds all rows where at least one year quantity value is an annotation
        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_annotations_temp;
        CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_annotations_temp (LIKE em_sta_powerusers_snapshot_temp INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP;
        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_annotations_temp
            SELECT *
            FROM em_sta_powerusers_snapshot_temp
            WHERE EXISTS (
                SELECT 1
                FROM unnest(ARRAY[%s]) AS val
                WHERE val = ANY($1)
            );$f$
            , time_series_column_list_strs[2])
        USING annotations_list;

        -- OLD CODE FOR STEP 3 (When the data was in portrait)
        -- DROP TABLE IF EXISTS em_sta_powerusers_snapshot_annotations_temp;
        -- CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_annotations_temp (LIKE em_sta_powerusers_snapshot_temp INCLUDING DEFAULTS INCLUDING CONSTRAINTS) ON COMMIT DROP;

        -- current_key_id := (SELECT t.key_id FROM em_sta_powerusers_snapshot_temp t ORDER BY t.key_id, t.year LIMIT 1);
        -- RAISE NOTICE 'the first row''s key is %', current_key_id;
        -- current_key_has_annotation := FALSE;
        -- current_rows_this_key := ARRAY[]::ggds_invdb.PU_EMISSION_RECORD[]; 
        
        -- i := 0;
        -- FOR current_record IN (SELECT * FROM em_sta_powerusers_snapshot_temp t ORDER BY t.key_id, t.year) LOOP -- loop through the temp table rows
        --     i := i + 1;
        --     --RAISE NOTICE 'Processing row number %, with key: %, and rownumber %', i, current_record.key_id, current_record.rownumber;
        --     IF current_key_id != current_record.key_id THEN -- when a new key is encountered
        --         --RAISE NOTICE 'found a new key. Was %, now its %', current_key_id, current_record.key_id;
        --         -- finalize actions for the previous key_id's data
        --         IF current_key_has_annotation = TRUE THEN -- if an annotation was found in this key
        --             FOREACH current_array_record IN ARRAY current_rows_this_key LOOP -- then dump the key id's data into the array
        --                 current_array_record.weighted_quantity := CASE WHEN current_array_record.weighted_quantity ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN NULL ELSE current_array_record.weighted_quantity END; -- convert numerics to NULL
        --                 INSERT INTO em_sta_powerusers_snapshot_annotations_temp
        --                 VALUES (current_array_record.key_id, current_array_record.sector, current_array_record.subsector,
        --                         current_array_record.category, current_array_record.sub_category_1, current_array_record.sub_category_2, current_array_record.sub_category_3, 
        --                         current_array_record.sub_category_4, current_array_record.sub_category_5, current_array_record.carbon_pool, current_array_record.fuel1, 
        --                         current_array_record.fuel2, current_array_record.geo_ref, current_array_record.exclude, current_array_record.crt_code, current_array_record.id, 
        --                         current_array_record.cbi_activity, current_array_record.units, current_array_record.ghg, current_array_record.gwp, current_array_record.ghg_category, current_array_record.year, current_array_record.weighted_quantity, 
        --                         current_array_record.e_or_i, current_array_record.rownumber);
        --             END LOOP;
        --         END IF;
        --         -- now setup the new key
        --         current_key_id := current_record.key_id;
        --         current_key_has_annotation := current_record.weighted_quantity = ANY(annotations_list);
        --         current_rows_this_key := ARRAY[current_record];
        --     ELSE
        --         --RAISE NOTICE 'continuing on the same key %', current_key_id;
        --         current_rows_this_key := array_append(current_rows_this_key, current_record);
        --         IF current_record.weighted_quantity = ANY(annotations_list) THEN
        --             current_key_has_annotation := TRUE;
        --         END IF;
        --     END IF;
        --     --RAISE NOTICE '%', current_rows_this_key;
        -- END LOOP; 
        -- -- finalize actions for the last key id
        -- IF current_key_has_annotation = TRUE THEN -- if an annotation was found in this key
        --     FOREACH current_array_record IN ARRAY current_rows_this_key LOOP -- then dump the key id's data into the array
        --         current_array_record.weighted_quantity := CASE WHEN current_array_record.weighted_quantity ~ '^[-+]?[0-9]+(\.[0-9]*)?$' THEN NULL ELSE current_array_record.weighted_quantity END; -- convert numerics to NULL
        --         INSERT INTO em_sta_powerusers_snapshot_annotations_temp
        --         VALUES (current_array_record.key_id, current_array_record.sector, current_array_record.subsector,
        --                 current_array_record.category, current_array_record.sub_category_1, current_array_record.sub_category_2, current_array_record.sub_category_3, 
        --                 current_array_record.sub_category_4, current_array_record.sub_category_5, current_array_record.carbon_pool, current_array_record.fuel1, 
        --                 current_array_record.fuel2, current_array_record.geo_ref, current_array_record.exclude, current_array_record.crt_code, current_array_record.id, 
        --                 current_array_record.cbi_activity, current_array_record.units, current_array_record.ghg, current_array_record.gwp, current_array_record.ghg_category, current_array_record.year, current_array_record.weighted_quantity, 
        --                 current_array_record.e_or_i, current_array_record.rownumber);
        --     END LOOP;
        -- END IF;

        -- set all annotations in the temp table to '0'.
        FOR year_value IN 1990..max_time_series LOOP
            EXECUTE format('
                UPDATE em_sta_powerusers_snapshot_temp SET "Y%s" = ''0.0'' 
                WHERE "Y%s" = ANY($1);', year_value, year_value)
            USING annotations_list;
        END LOOP;

        -- convert the time series values to numerics in the temp table
        EXECUTE format('
            ALTER TABLE em_sta_powerusers_snapshot_temp %s;', 
            time_series_column_list_strs[7]);

        -- OLD CODE (When the data was in portrait)
        -- UPDATE em_sta_powerusers_snapshot_temp t SET weighted_quantity = '0' WHERE t.weighted_quantity = ANY(annotations_list);


        -- report a table that show how many duplicates each row has: 
        DROP TABLE IF EXISTS ggds_invdb.combined_table_duplicated_rows;
        EXECUTE format($f$CREATE TABLE ggds_invdb.combined_table_duplicated_rows AS (
            SELECT t.*, COUNT(*) as duplicate_count
            FROM em_sta_powerusers_snapshot_temp t
            GROUP BY t.key_id,
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
            t.geo_ref,
            t.exclude,
            t.crt_code,
            t.id,
            t.cbi_activity,
            t.units,
            t.ghg,
            t.gwp,
            t.ghg_category,
            %s, 
            t.e_or_i,
            t.rownumber
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        );$f$,
        time_series_column_list_strs[3]);

        
        -- 4) redact chemicals according to the dim_redacted_ghg chemicals
        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_redactions_temp;
        EXECUTE format($f$CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_redactions_temp AS (
            SELECT t.key_id, t.sector, t.subsector, 
                t.category, t.sub_category_1, 
                t.sub_category_2, t.sub_category_3, t.sub_category_4, t.sub_category_5, 
                t.carbon_pool, t.fuel1, t.fuel2, t.geo_ref, t.exclude, 
                t.crt_code, t.id, t.cbi_activity, t.units, redacted_ghgs.redacted_ghg_name as ghg, t.gwp, t.ghg_category,
                %s, t.e_or_i, NULL::INTEGER as rownumber
            FROM (SELECT DISTINCT * FROM em_sta_powerusers_snapshot_temp) t
                LEFT JOIN ggds_invdb.dim_ghg ghgs ON ghgs.ghg_longname = t.ghg
                JOIN ggds_invdb.dim_redacted_ghg redacted_ghgs ON ghgs.ghg_id = redacted_ghgs.ghg_id 
            WHERE t.category = 'Substitution of Ozone Depleting Substances'
            GROUP BY t.key_id, t.sector, t.subsector,
                    t.category, t.sub_category_1, t.sub_category_2,
                    t.sub_category_3, t.sub_category_4, t.sub_category_5,
                    t.carbon_pool, t.fuel1, t.fuel2, t.geo_ref, t.exclude,
                    t.crt_code, t.id, t.cbi_activity, t.units, redacted_ghgs.redacted_ghg_name, t.gwp,t.ghg_category,
                    t.e_or_i, t.rownumber
            ORDER BY t.rownumber
        );$f$, 
        time_series_column_list_strs[4]); 

        -- obfuscate gwp values
        UPDATE em_sta_powerusers_snapshot_redactions_temp SET gwp = 1;

        -- temp table to hold combined redaction data
        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_redactions_temp_combined;
        EXECUTE format($f$
            CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_redactions_temp_combined AS
            SELECT 
                MIN(t.key_id) as key_id,
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
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                MIN(t.rownumber) as rownumber
            FROM em_sta_powerusers_snapshot_redactions_temp t
            GROUP BY 
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
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                t.e_or_i;$f$, 
            time_series_column_list_strs[4]);

        SELECT COUNT(*) INTO row_count FROM em_sta_powerusers_snapshot_temp;
        RAISE NOTICE 'Before the redaction, there are % rows.', row_count;

        -- delete the redacted individual rows from the temp table (this works, just make sure dim_redacted_ghg is up-to-date!)
        DELETE FROM em_sta_powerusers_snapshot_temp t
        WHERE EXISTS (
            SELECT dim_ghg.ghg_longname as ghg, redacted_ghgs.category as category
            FROM     ggds_invdb.dim_redacted_ghg redacted_ghgs
                JOIN ggds_invdb.dim_ghg dim_ghg ON dim_ghg.ghg_id = redacted_ghgs.ghg_id
            WHERE t.ghg = dim_ghg.ghg_longname
                  AND t.category = redacted_ghgs.category 
        );

        SELECT COUNT(*) INTO row_count FROM em_sta_powerusers_snapshot_temp;
        RAISE NOTICE 'After the redaction,  there are % rows.', row_count;

        -- insert the redacted aggregations into the temp table
        INSERT INTO em_sta_powerusers_snapshot_temp
        SELECT * FROM em_sta_powerusers_snapshot_redactions_temp_combined;

        -- 5) Inserts / Update in PU combined – none at this time

        -- 6) split the temp table into industry and econsect
        DROP TABLE IF EXISTS ggds_invdb.INVDB_505_1_1_before_all_PU_combined;
        CREATE TABLE ggds_invdb.INVDB_505_1_1_before_all_PU_combined AS
        TABLE em_sta_powerusers_snapshot_temp;

        DROP TABLE IF EXISTS em_sta_powerusers_snapshot_econsect_temp;
        CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_econsect_temp AS
        SELECT * FROM em_sta_powerusers_snapshot_temp;

        ALTER TABLE em_sta_powerusers_snapshot_econsect_temp 
        ADD COLUMN econ_sector VARCHAR(100),
        ADD COLUMN econ_subsector VARCHAR(100);

        DELETE FROM em_sta_powerusers_snapshot_temp t -- isolate industry data
        WHERE t.e_or_i = 'Econ Sect';

        DELETE FROM em_sta_powerusers_snapshot_econsect_temp t -- isolate econ_sector data
        WHERE t.e_or_i = 'Industry';

        -- 7) Insert EconSect corrections
        -- 7a)	Corrections to all for Econ Sect Pivot sums to work (Insert Subcategory1='Mobile Non-Highway Other' 
        --      or Subcategory1='Mobile Non-Highway Construction' or Subcategory1='Mobile Agricultural Equipment' in 
        --      Energy FFC Transportation; duplicate mobile, change sign for energy)
        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT 
                MD5('(''Energy'', ''Fossil Fuel Combustion'', ''Transportation'', ' || 
                    t.sub_category_1 || ', ' || 
                    t.sub_category_2 || ', ' || 
                    t.sub_category_3 || ', ' || 
                    t.sub_category_4 || ', ' || 
                    t.sub_category_5 || ', ' || 
                    t.carbon_pool || ', ' || 
                    t.fuel1 || ', ' || 
                    t.fuel2 || ', ' || 
                    CASE WHEN $1 = 2 THEN t.geo_ref ELSE '' END || ', ' || 
                    t.exclude || ', ' || 
                    t.crt_code || ', ' || 
                    t.id || ', ' || 
                    t.cbi_activity || ', ' || 
                    t.units || ', ' || 
                    t.ghg || ', ' || 
                    t.gwp::text || ')')::TEXT,
                'Energy',
                'Fossil Fuel Combustion',
                'Transportation',
                t.sub_category_1,
                t.sub_category_2,
                t.sub_category_3,
                t.sub_category_4,
                t.sub_category_5,
                t.carbon_pool,
                t.fuel1,
                t.fuel2,
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Transportation',
                'Mobile Combustion'
            FROM em_sta_powerusers_snapshot_econsect_temp t
            WHERE t.sub_category_1 = 'Mobile Non-Highway Farm Equipment' 
            AND (t.ghg = 'Methane' OR t.ghg = 'Nitrous Oxide');$f$, 
            time_series_column_list_strs[5])
        USING layer_id;

        -- Insertion added by INVDB-553
        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT 
                MD5('(''Agriculture'', ''Fossil Fuel Combustion'', ''Industrial'', ' || 
                    t.sub_category_1 || ', ' || 
                    t.sub_category_2 || ', ' || 
                    t.sub_category_3 || ', ' || 
                    t.sub_category_4 || ', ' || 
                    t.sub_category_5 || ', ' || 
                    t.carbon_pool || ', ' || 
                    t.fuel1 || ', ' || 
                    t.fuel2 || ', ' || 
                    CASE WHEN $1 = 2 THEN t.geo_ref ELSE '' END || ', ' || 
                    t.exclude || ', ' || 
                    t.crt_code || ', ' || 
                    t.id || ', ' || 
                    t.cbi_activity || ', ' || 
                    t.units || ', ' || 
                    t.ghg || ', ' || 
                    t.gwp::text || ')')::TEXT,
                'Agriculture',
                'Fossil Fuel Combustion',
                'Industrial',
                t.sub_category_1,
                t.sub_category_2,
                t.sub_category_3,
                t.sub_category_4,
                t.sub_category_5,
                t.carbon_pool,
                t.fuel1,
                t.fuel2,
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Industry',
                'Carbon Dioxide from Fossil Fuel Combustion'
            FROM em_sta_powerusers_snapshot_econsect_temp t
            WHERE t.sub_category_1 = 'Mobile Non-Highway Farm Equipment' AND t.ghg = 'Carbon Dioxide';$f$,
            time_series_column_list_strs[5])
        USING layer_id;

        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector='Agriculture', econ_subsector='Carbon Dioxide from Fossil Fuel Combustion', 
            sector='Agriculture', subsector='Fossil Fuel Combustion', category='Agriculture' 
        WHERE t.category='External Source' AND t.sub_category_1='Mobile Non-Highway Farm Equipment';
        
        -- sub_category_1 = 'Mobile Agricultural Equipment' was renamed to 'Mobile Non-Highway Farm Equipment' in 2024
        -- EXECUTE format('
        --     INSERT INTO em_sta_powerusers_snapshot_econsect_temp
        --     SELECT 
        --         MD5('(''Energy'', ''Fossil Fuel Combustion'', ''Transportation'', ' || 
        --             t.sub_category_1 || ', ' || 
        --             t.sub_category_2 || ', ' || 
        --             t.sub_category_3 || ', ' || 
        --             t.sub_category_4 || ', ' || 
        --             t.sub_category_5 || ', ' || 
        --             t.carbon_pool || ', ' || 
        --             t.fuel1 || ', ' || 
        --             t.fuel2 || ', ' || 
        --             CASE WHEN $1 = 2 THEN t.geo_ref ELSE '' END || ', ' || 
        --             t.exclude || ', ' || 
        --             t.crt_code || ', ' || 
        --             t.id || ', ' || 
        --             t.cbi_activity || ', ' || 
        --             t.units || ', ' || 
        --             t.ghg || ', ' || 
        --             t.gwp::text || ')')::TEXT,''Energy'',
        --             ''Fossil Fuel Combustion'',
        --             ''Transportation'',
        --             t.sub_category_1,
        --             t.sub_category_2,
        --             t.sub_category_3,
        --             t.sub_category_4,
        --             t.sub_category_5,
        --             t.carbon_pool,
        --             t.fuel1,
        --             t.fuel2,
        --             CASE WHEN $1 = 2 THEN t.geo_ref ELSE '''' END,
        --             t.exclude,
        --             t.crt_code,
        --             t.id,
        --             t.cbi_activity,
        --             t.units,
        --             t.ghg,
        --             t.gwp::text)
        --         )::TEXT as key_id,
        --         ''Energy'',
        --         t.subsector,
        --         ''Agriculture'',
        --         t.sub_category_1,
        --         t.sub_category_2,
        --         t.sub_category_3,
        --         t.sub_category_4,
        --         t.sub_category_5,
        --         t.carbon_pool,
        --         t.fuel1,
        --         t.fuel2,
        --         t.geo_ref,
        --         t.exclude,
        --         t.crt_code,
        --         t.id,
        --         t.cbi_activity,
        --         t.units,
        --         t.ghg,
        --         t.gwp,
        --         t.ghg_category,
        --         %s,
        --         t.e_or_i,
        --         t.rownumber,
        --         ''Industry'',
        --         ''Fossil Fuel Combustion''
        --     FROM em_sta_powerusers_snapshot_temp t
        --     WHERE t.sub_category_1 = ''Mobile Agricultural Equipment'' AND t.ghg = ''Carbon Dioxide'';',
        --     time_series_column_list_strs[5])
        -- USING layer_id;

        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT 
                MD5('(''Energy'', ''Fossil Fuel Combustion'', ''Transportation'', ' || 
                    t.sub_category_1 || ', ' || 
                    t.sub_category_2 || ', ' || 
                    t.sub_category_3 || ', ' || 
                    t.sub_category_4 || ', ' || 
                    t.sub_category_5 || ', ' || 
                    t.carbon_pool || ', ' || 
                    t.fuel1 || ', ' || 
                    t.fuel2 || ', ' || 
                    CASE WHEN $1 = 2 THEN t.geo_ref ELSE '' END || ', ' || 
                    t.exclude || ', ' || 
                    t.crt_code || ', ' || 
                    t.id || ', ' || 
                    t.cbi_activity || ', ' || 
                    t.units || ', ' || 
                    t.ghg || ', ' || 
                    t.gwp::text || ')')::TEXT,
                'Energy',
                'Fossil Fuel Combustion',
                'Transportation',
                t.sub_category_1,
                t.sub_category_2,
                t.sub_category_3,
                t.sub_category_4,
                t.sub_category_5,
                t.carbon_pool,
                t.fuel1,
                t.fuel2,
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Transportation',
                'Mobile Combustion'
            FROM em_sta_powerusers_snapshot_econsect_temp t
            WHERE t.sub_category_1 IN ('Mobile Non-Highway Construction', 'Mobile Non-Highway Other');$f$,
            time_series_column_list_strs[5])
        USING layer_id;  
        

        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT 
                MD5('(''Agriculture'', ''Fossil Fuel Combustion'', ''Industrial'', ' || 
                    t.sub_category_1 || ', ' || 
                    t.sub_category_2 || ', ' || 
                    t.sub_category_3 || ', ' || 
                    t.sub_category_4 || ', ' || 
                    t.sub_category_5 || ', ' || 
                    t.carbon_pool || ', ' || 
                    t.fuel1 || ', ' || 
                    t.fuel2 || ', ' || 
                    CASE WHEN $1 = 2 THEN t.geo_ref ELSE '' END || ', ' || 
                    t.exclude || ', ' || 
                    t.crt_code || ', ' || 
                    t.id || ', ' || 
                    t.cbi_activity || ', ' || 
                    t.units || ', ' || 
                    t.ghg || ', ' || 
                    t.gwp::text || ')')::TEXT,
                'Agriculture',
                'Fossil Fuel Combustion',
                'Industrial',
                t.sub_category_1,
                t.sub_category_2,
                t.sub_category_3,
                t.sub_category_4,
                t.sub_category_5,
                t.carbon_pool,
                t.fuel1,
                t.fuel2,
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Industry',
                'Stationary Combustion'
            FROM em_sta_powerusers_snapshot_econsect_temp t
            WHERE t.sub_category_1 = 'AG Stationary';$f$,
            time_series_column_list_strs[5])
        USING layer_id;  
        

        DROP TABLE IF EXISTS ggds_invdb.INVDB_505_1_2_econsect_after_negative_inserts;
        CREATE TABLE ggds_invdb.INVDB_505_1_2_econsect_after_negative_inserts AS 
        TABLE em_sta_powerusers_snapshot_econsect_temp;
        
        -- 7b) Carbonates adjustments to Econ Sect (split 50/50 between electric power and industry)
        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT DISTINCT 
                t.key_id,
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
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Electric Power Industry',
                'Other Process Uses of Carbonates' -- the " -- new" suffix is temporary, see step 7b iii
            FROM em_sta_powerusers_snapshot_temp t
            WHERE t.category = 'Other Process Uses of Carbonates';$f$,
            time_series_column_list_strs[6]);


        EXECUTE format($f$
            INSERT INTO em_sta_powerusers_snapshot_econsect_temp
            SELECT DISTINCT t.key_id,
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
                t.geo_ref,
                t.exclude,
                t.crt_code,
                t.id,
                t.cbi_activity,
                t.units,
                t.ghg,
                t.gwp,
                t.ghg_category,
                %s,
                t.e_or_i,
                t.rownumber,
                'Industry',
                'Other Process Uses of Carbonates' -- the " -- new" suffix is temporary, see step 7b iii
            FROM em_sta_powerusers_snapshot_temp t
            WHERE t.category = 'Other Process Uses of Carbonates';$f$,
            time_series_column_list_strs[6]);

        DROP TABLE IF EXISTS ggds_invdb.INVDB_505_1_3_econsect_after_half_and_before_delete;
        CREATE TABLE ggds_invdb.INVDB_505_1_3_econsect_after_half_and_before_delete AS 
        TABLE em_sta_powerusers_snapshot_econsect_temp;

        -- 7b iii) Delete 100% of old rows where category = 'Other Process Uses of Carbonates'
        -- edited by INVDB-508
        DELETE FROM em_sta_powerusers_snapshot_econsect_temp t
        WHERE t.category = 'Other Process Uses of Carbonates' AND t.econ_sector IS NULL;

        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_subsector = 'Other Process Uses of Carbonates'
        WHERE t.econ_subsector = 'Other Process Uses of Carbonates -- new';

        DROP TABLE IF EXISTS ggds_invdb.INVDB_505_1_4_econsect_after_delete;
        CREATE TABLE ggds_invdb.INVDB_505_1_4_econsect_after_delete AS 
        TABLE em_sta_powerusers_snapshot_econsect_temp;

        -- 8) Add econ sector and econ subsector throughout
        --a
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Transportation', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Transportation' and t.ghg = 'Carbon Dioxide';
        --b
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Transportation', econ_subsector = 'Substitution of Ozone Depleting Substances' 
        WHERE t.sub_category_1 = 'ODS Subst Transportation'; 
        --c
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Transportation', econ_subsector = 'Mobile Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Transportation' and t.ghg = 'Methane';
        --d
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Transportation', econ_subsector = 'Mobile Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Transportation' and t.ghg = 'Nitrous Oxide';
        --e
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Transportation', econ_subsector = 'Non-Energy Use of Fuels' 
        WHERE t.subsector= 'Non-Energy Uses of Fossil Fuels' and t.category = 'Transportation';
        --f
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Electric Power Industry', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Electricity Generation' and t.ghg = 'Carbon Dioxide';
        --g
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Electric Power Industry', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Electricity Generation' and t.ghg = 'Methane';
        --h
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Electric Power Industry', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Electricity Generation' and t.ghg = 'Nitrous Oxide';
        --i
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Electric Power Industry', econ_subsector = 'Incineration of Waste' 
        WHERE t.subsector = 'Incineration of Waste';
        --j
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Electric Power Industry', econ_subsector = 'Electrical Equipment' 
        WHERE t.category = 'Electrical Equipment';
        --k
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Industrial' and t.ghg = 'Carbon Dioxide';
        --l
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion', category= 'Industry' 
        WHERE t.sector = 'Energy' and t.sub_category_1 = 'Mobile Agricultural Equipment' and t.ghg = 'Carbon Dioxide';
        --m
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Mobile Combustion' 
        WHERE t.category= 'Industry' and t.sub_category_1 IN ('Mobile Non-Highway Construction', 'Mobile Non-Highway Other');
        --n
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Natural Gas Systems' 
        WHERE t.category = 'Natural Gas Systems';
        --o
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Non-Energy Use of Fuels' 
        WHERE t.subsector = 'Non-Energy Uses of Fossil Fuels' and t.category = 'Industrial';
        --p
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Petroleum Systems' 
        WHERE t.category = 'Petroleum Systems';
        --q
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Coal Mining' 
        WHERE t.category = 'Coal Mining';
        --r
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Iron and Steel Production' 
        WHERE t.category = 'Iron and Steel Production';
        --s
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Cement Production' 
        WHERE t.category = 'Cement Production';
        --t
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Substitution of Ozone Depleting Substances' 
        WHERE t.sub_category_1 = 'ODS Subst Industry';
        --u
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Petrochemical Production' 
        WHERE t.category = 'Petrochemical Production';
        --v
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Ammonia Production' 
        WHERE t.category = 'Ammonia Production';
        --w
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Lime Production' 
        WHERE t.category = 'Lime Production';
        --x
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Adipic Acid Production' 
        WHERE t.category = 'Adipic Acid Production';
        --y
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Nitric Acid Production' 
        WHERE t.category = 'Nitric Acid Production';
        --z
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Abandoned Oil and Gas Wells' 
        WHERE t.category = 'Abandoned Wells';
        --aa
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Wastewater Treatment' 
        WHERE t.subsector= 'Wastewater Treatment and Discharge' and t.category = 'Industrial';
        --bb
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Abandoned Underground Coal Mines' 
        WHERE t.category = 'Abandoned Coal Mines';
        --cc
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Electronics Industry' 
        WHERE t.subsector= 'Electronics Industry';
        --dd
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Carbon Dioxide Consumption' 
        WHERE t.category = 'Carbon Dioxide Consumption';
        --ee
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Nitrous Oxide from Product Uses' 
        WHERE t.category= 'Nitrous Oxide from Product Uses';
        --ff
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Industrial' and t.ghg = 'Methane';
        --gg
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Industrial' and t.ghg = 'Nitrous Oxide';
        --hh
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Urea Consumption for Non-Agricultural Purposes' 
        WHERE t.category = 'Urea Consumption for Non-Agricultural Uses';
        --ii
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Fluorochemical Production' 
        WHERE t.category = 'Fluorochemical Production';
        --jj
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Aluminum Production' 
        WHERE t.category = 'Aluminum Production';
        --kk
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Ferroalloy Production' 
        WHERE t.category = 'Ferroalloy Production';
        --ll
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Soda Ash Production' 
        WHERE t.category = 'Soda Ash Production';
        --mm
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Titanium Dioxide Production' 
        WHERE t.category = 'Titanium Dioxide Production';
        --nn
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Caprolactam, Glyoxal and Glyoxylic Acid Production' 
        WHERE t.category = 'Caprolactam, Glyoxal* and Glyoxylic Acid Production';
        --oovb
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Glass Production' 
        WHERE t.category = 'Glass Production';
        --pp
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Magnesium Production' 
        WHERE t.category = 'Magnesium Production';
        --qq
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Zinc Production' 
        WHERE t.category = 'Zinc Production';
        --rr
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Phosphoric Acid Production' 
        WHERE t.category = 'Phosphoric Acid Production';
        --ss
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Lead Production' 
        WHERE t.category = 'Lead Production';
        --tt
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Landfills - Industrial' 
        WHERE t.category = 'Landfills' and t.sub_category_1 = 'Industrial Waste Landfills Emissions';
        --uu
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Carbide Production and Consumption' 
        WHERE t.category = 'Silicon Carbide Production and Consumption';
        --vv
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Nitrous Oxide from Agricultural Soil Management' 
        WHERE t.category = 'Agricultural Soil Management';
        --ww
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Enteric Fermentation' 
        WHERE t.category = 'Enteric Fermentation';
        --xx
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Manure Management' 
        WHERE t.category = 'Manure Management';
        --yy
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Agriculture' and t.ghg = 'Carbon Dioxide';
        --zz
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Rice Cultivation' 
        WHERE t.category = 'Rice Cultivation';
        --aaa
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Urea Fertilization' 
        WHERE t.category = 'Urea Fertilization';
        --bbb
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Liming' 
        WHERE t.category = 'Liming';
        --ccc
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Field Burning of Agricultural Residues' 
        WHERE t.category = 'Field Burning of Agricultural Residues';
        --ddd
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Mobile Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Agriculture' and t.ghg = 'Methane';
        --eee
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Mobile Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Agriculture' and t.ghg = 'Nitrous Oxide';
        --fff
        UPDATE em_sta_powerusers_snapshot_econsect_temp t 
        SET econ_sector = 'Agriculture', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion'  and t.sub_category_1 = 'AG Stationary' and t.econ_subsector IS NULL; 
        --ggg
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Commercial' and t.ghg = 'Carbon Dioxide';
        --hhh
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Landfills - Municipal' 
        WHERE t.category = 'Landfills' and t.sub_category_1 = 'MSW Landfills Generation Emissions';
        --iii
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Substitution of Ozone Depleting Substances' 
        WHERE t.sub_category_1 = 'ODS Subst Commercial'; 
        --jjj
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Wastewater Treatment' 
        WHERE t.subsector= 'Wastewater Treatment and Discharge' and t.category = 'Domestic';
        --kkk
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Composting' 
        WHERE t.category = 'Composting';
        --lll
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Anaerobic Digestion at Biogas Facilities' 
        WHERE t.category = 'Anaerobic Digestion at Biogas Facilities';
        --mmm
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Commercial' and t.ghg = 'Methane';
        --nnn
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Commercial' and t.ghg = 'Nitrous Oxide';
        --ooo
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Residential', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Residential' and t.ghg = 'Carbon Dioxide';
        --ppp
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Residential', econ_subsector = 'Substitution of Ozone Depleting Substances' 
        WHERE t.sub_category_1 = 'ODS Subst Residential'; 
        --qqq
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Residential', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Residential' and t.ghg = 'Methane';
        --rrr
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Residential', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'Residential' and t.ghg = 'Nitrous Oxide';
        --sss
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'U.S. Territories', econ_subsector = 'Carbon Dioxide from Fossil Fuel Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'US Territories' and t.ghg = 'Carbon Dioxide';
        --ttt
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'U.S. Territories', econ_subsector = 'Non-Energy Use of Fuels' 
        WHERE t.subsector= 'Non-Energy Uses of Fossil Fuels' and t.category = 'US Territories';
        --uuu
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'U.S. Territories', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'US Territories' and t.ghg = 'Methane';
        --vvv
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'U.S. Territories', econ_subsector = 'Stationary Combustion' 
        WHERE t.subsector = 'Fossil Fuel Combustion' and t.category = 'US Territories' and t.ghg = 'Nitrous Oxide';
        --www
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'LULUCF Sector Net Total', econ_subsector = ''
        WHERE t.sector = 'Land Use, Land-Use Change and Forestry';
        --xxx
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'SF6 and PFCs from Other Product Use' 
        WHERE t.category = 'SF6 and PFCs from other product use';
        --yyy
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Carbide Production and Consumption' 
        WHERE t.category = 'Carbide Production and Consumption';
        --zzz
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'Hydrogen Production' 
        WHERE t.sub_category_1 = 'Hydrogen Production';
        --aaaa
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Industry', econ_subsector = 'N2O from Product Uses' 
        WHERE t.category = 'N2O from Product Uses';
        --bbbb -- Added by INVDB-505
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Agriculture', econ_subsector = 'Stationary Combustion'
        WHERE t.sub_category_1 = 'AG Stationary' AND t.sector = 'Agriculture';
        --cccc -- Added by INVDB-550 (item 2)
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector=t.sub_category_1, econ_subsector='Substitution of Ozone Depleting Substances', 
            sector='Industrial Processes and Product Use', subsector='Product uses as substitutes for ODS', 
            category='Substitution of Ozone Depleting Substances' 
        WHERE t.category = 'External Source' AND t.sub_category_1 IN ('Commercial', 'Residential', 'Industry', 'Transportation');
        --dddd -- Added by INVDB-550 (item 2)
        UPDATE em_sta_powerusers_snapshot_econsect_temp t
        SET econ_sector = 'Commercial', econ_subsector = 'Landfills - Municipal' 
        WHERE t.sub_category_1 = 'MSW Landfills Emissions';

        -- Some final deletions
        --- added by INVDB-502
        DELETE FROM em_sta_powerusers_snapshot_econsect_temp t
        WHERE t.category = 'Landfills' AND t.sub_category_1 IN ('Energy Recovery', 'Flaring','Industrial Oxidation');
        
        DELETE FROM em_sta_powerusers_snapshot_econsect_temp t
        WHERE t.category = 'Wastewater Treatment and Discharge' AND t.category='Other';
        -- added by INVDB-504
        DELETE FROM em_sta_powerusers_snapshot_econsect_temp t
        WHERE t.category ='External Source' AND TRIM(t.sub_category_1) IN ('Aviation', 'Marine', 'Mobile Non-Highway Aircraft', 
                                                                            'Mobile Non-Highway Locomotives', 'Mobile Non-Highway Ships and Boats',
                                                                            'Other', 'Other Annex', 'Other Annex - Subtraction', 'Road Highway Vehicles');
        -- drop no-longer-needed processing columns
        ALTER TABLE em_sta_powerusers_snapshot_temp 
        DROP COLUMN e_or_i, DROP COLUMN rownumber;
        ALTER TABLE em_sta_powerusers_snapshot_econsect_temp 
        DROP COLUMN e_or_i, DROP COLUMN rownumber;
        ALTER TABLE em_sta_powerusers_snapshot_annotations_temp 
        DROP COLUMN e_or_i, DROP COLUMN rownumber;       

        -- 10) In both Industry and Econ_Sect aggregated by full key to eliminate the geo_ref field for national case only
        IF layer_id = 1 THEN
            -- for industry
            DROP TABLE IF EXISTS em_sta_powerusers_snapshot_temp_save;
            CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_temp_save AS SELECT * FROM em_sta_powerusers_snapshot_temp;
            
            DELETE FROM em_sta_powerusers_snapshot_temp;
            ALTER TABLE em_sta_powerusers_snapshot_temp 
            DROP COLUMN geo_ref;

            EXECUTE format($f$
                INSERT INTO em_sta_powerusers_snapshot_temp
                SELECT  t.key_id, t.sector, t.subsector, t.category, t.sub_category_1, 
                        t.sub_category_2, t.sub_category_3, t.sub_category_4, 
                        t.sub_category_5, t.carbon_pool, t.fuel1, t.fuel2, 
                        t.exclude, t.crt_code, t.id, t.cbi_activity, t.units, 
                        t.ghg, t.gwp, t.ghg_category, %s
                FROM em_sta_powerusers_snapshot_temp_save t
                GROUP BY t.key_id, t.sector, t.subsector, t.category, t.sub_category_1, 
                        t.sub_category_2, t.sub_category_3, t.sub_category_4, 
                        t.sub_category_5, t.carbon_pool, t.fuel1, t.fuel2, 
                        t.exclude, t.crt_code, t.id, t.cbi_activity, t.units, 
                        t.ghg, t.gwp, t.ghg_category;$f$, 
                time_series_column_list_strs[4]);
            

            -- step 10 for economic sector
            DROP TABLE IF EXISTS em_sta_powerusers_snapshot_econsect_temp_save;
            CREATE TEMPORARY TABLE em_sta_powerusers_snapshot_econsect_temp_save AS SELECT * FROM em_sta_powerusers_snapshot_econsect_temp;
            
            DELETE FROM em_sta_powerusers_snapshot_econsect_temp;
            ALTER TABLE em_sta_powerusers_snapshot_econsect_temp 
            DROP COLUMN geo_ref;

            EXECUTE format($f$
                INSERT INTO em_sta_powerusers_snapshot_econsect_temp
                SELECT  t.key_id, t.sector, t.subsector, t.category, t.sub_category_1, 
                        t.sub_category_2, t.sub_category_3, t.sub_category_4, 
                        t.sub_category_5, t.carbon_pool, t.fuel1, t.fuel2, 
                        t.exclude, t.crt_code, t.id, t.cbi_activity, t.units, 
                        t.ghg, t.gwp, t.ghg_category, %s, 
                        t.econ_sector, t.econ_subsector
                FROM em_sta_powerusers_snapshot_econsect_temp_save t
                GROUP BY t.key_id, t.sector, t.subsector, t.category, t.sub_category_1, 
                            t.sub_category_2, t.sub_category_3, t.sub_category_4, 
                            t.sub_category_5, t.carbon_pool, t.fuel1, t.fuel2, 
                            t.exclude, t.crt_code, t.id, t.cbi_activity, t.units, 
                            t.ghg, t.gwp, t.ghg_category, t.econ_sector, t.econ_subsector;$f$,
                time_series_column_list_strs[4]);
        END IF;

        -- return data and clean up
        -- print the row counts
        SELECT COUNT(*) INTO row_count FROM em_sta_powerusers_snapshot_temp;
        RAISE NOTICE 'the final landscape industry table is made. It contains % rows.', row_count;

        SELECT COUNT(*) INTO row_count FROM em_sta_powerusers_snapshot_econsect_temp;
        RAISE NOTICE 'the final landscape econsect table is made. It contains % rows.', row_count;

        

        IF layer_id = 1 THEN
            return_json := json_build_object( 
                'Data by UNFCCC-IPCC Sectors', (
                    SELECT json_agg(row_to_json(t1)) FROM (
                    SELECT * FROM em_sta_powerusers_snapshot_temp
                    ) t1
                ),
                'Data by Economic Sectors', (
                    SELECT json_agg(row_to_json(t2)) FROM (
                    SELECT * FROM em_sta_powerusers_snapshot_econsect_temp
                    ) t2
                )
            );
        ELSE
            return_json := json_build_object( 
                'Data by UNFCCC-IPCC Sectors', (
                    SELECT json_agg(row_to_json(t1)) FROM (
                    SELECT * FROM em_sta_powerusers_snapshot_temp
                    ) t1
                ),
                'Data by Economic Sectors', (
                    SELECT json_agg(row_to_json(t2)) FROM (
                    SELECT * FROM em_sta_powerusers_snapshot_econsect_temp
                    ) t2
                ),
                'Annotations', (
                    SELECT json_agg(row_to_json(t3)) FROM (
                    SELECT * FROM ggds_invdb.em_sta_powerusers_snapshot_annotations_temp
                    ) t3
                ) 
            );
        END IF;

        UPDATE ggds_invdb.publication_object po
        SET raw_data = to_json(return_json)::TEXT
        WHERE pub_object_id = input_pub_object_id;
        
        RAISE NOTICE 'Power User Report Complete!';
    END
    $function$;

    --national TEST test case
    --  select *
    --  from ggds_invdb.EM_STA_POWERUSERS_SNAPSHOT(240, 12, 1)