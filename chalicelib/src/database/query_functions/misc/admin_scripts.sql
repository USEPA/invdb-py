    -- test with results    
    -- SELECT * FROM ggds_invdb.delete_target_year(13);

CREATE OR REPLACE FUNCTION ggds_invdb.delete_target_year(target_pub_year_id integer)
RETURNS BOOLEAN
AS $BODY$
    DECLARE
        target_publication_year INTEGER;
    BEGIN
        SELECT pub_year INTO target_publication_year
        FROM ggds_invdb.dim_publication_year
        WHERE pub_year_id = target_pub_year_id;

        -- 1: dim_excel_report
        DELETE FROM ggds_invdb.dim_excel_report
        WHERE reporting_year = target_publication_year;

        -- 2: dim_qc_comp_report_rows
        DELETE FROM ggds_invdb.dim_qc_comp_report_row
        WHERE qc_report_id IN (
            SELECT qc_report_id 
            FROM ggds_invdb.dim_qc_report
            WHERE reporting_year = target_publication_year
        );

        -- 3: dim_qc_report
        DELETE FROM ggds_invdb.dim_qc_report
        WHERE reporting_year = target_publication_year;

        -- 4: dim_emissionsQC_load_target
        DELETE FROM ggds_invdb.dim_emissionsQC_load_target
        WHERE report_row_id IN (
            SELECT report_row_id 
            FROM ggds_invdb.dim_report_row
            WHERE report_id IN (
                SELECT report_id 
                FROM ggds_invdb.dim_report
                WHERE reporting_year = target_publication_year
            )
        );

        -- 5: dim_report_row
        DELETE FROM ggds_invdb.dim_report_row
        WHERE report_id IN (
            SELECT report_id 
            FROM ggds_invdb.dim_report
            WHERE reporting_year = target_publication_year
        );

        -- 6: dim_report
        DELETE FROM ggds_invdb.dim_report
        WHERE reporting_year = target_publication_year;

        -- 7: dim_source_name
        DELETE FROM ggds_invdb.dim_source_name
        WHERE pub_year_id = target_pub_year_id;

        -- 8: dim_publication_year
        DELETE FROM ggds_invdb.dim_publication_year
        WHERE pub_year_id = target_pub_year_id;

        -- 9: dim_layer
        WITH max_pub_year_fetch AS (
            SELECT max(pub_year) as max_pub_year
            FROM ggds_invdb.dim_publication_year
        )
        UPDATE ggds_invdb.dim_layer
        SET default_year = mpyf.max_pub_year
        FROM max_pub_year_fetch mpyf;

        RETURN TRUE;
    END
    $BODY$ LANGUAGE PLPGSQL;

    -- test with results    
    -- SELECT * FROM ggds_invdb.delete_target_year(13);





CREATE OR REPLACE FUNCTION ggds_invdb.initialize_target_year(input_user_id integer, input_target_pub_year_id integer default null, input_source_pub_year_id integer default null)
RETURNS JSONB
AS $BODY$
    DECLARE
        target_pub_year_id INTEGER;
        source_pub_year_id INTEGER;
        target_publication_year INTEGER;
        source_publication_year INTEGER;
        source_max_time_series INTEGER;

        current_max_pub_year_id INTEGER;
        current_max_default_year INTEGER;

        source_to_target_source_name_id_obj JSONB;
        source_to_target_report_id_obj JSONB;
        source_to_target_report_row_id_obj JSONB;
        source_to_target_qc_report_id_obj JSONB;

        current_record RECORD;
        next_id INTEGER;
    BEGIN
        -- find the current max 
        SELECT MAX(pub_year_id) INTO current_max_pub_year_id
        FROM ggds_invdb.dim_publication_year;

        
        target_pub_year_id = COALESCE(input_target_pub_year_id, current_max_pub_year_id + 1);
        source_pub_year_id = COALESCE(input_source_pub_year_id, current_max_pub_year_id);
        
        SELECT pub_year, max_time_series INTO source_publication_year, source_max_time_series
        FROM ggds_invdb.dim_publication_year
        WHERE pub_year_id = source_pub_year_id;

        -- 1: dim_publication_year
        IF NOT EXISTS(select 1 from ggds_invdb.dim_publication_year where pub_year_id = target_pub_year_id)
        THEN
            INSERT INTO ggds_invdb.dim_publication_year (pub_year_id, pub_year, gwp_column, max_time_series)
            SELECT target_pub_year_id, target_pub_year_id + (source_publication_year - source_pub_year_id), t.gwp_column, target_pub_year_id + (source_max_time_series - source_pub_year_id)
            FROM ggds_invdb.dim_publication_year t
            WHERE t.pub_year_id = source_pub_year_id;
        END IF;

        SELECT pub_year INTO target_publication_year        
        FROM ggds_invdb.dim_publication_year
        WHERE pub_year_id = target_pub_year_id;


        -- 2: dim_layer
        SELECT max(default_year) INTO current_max_default_year
        FROM ggds_invdb.dim_layer;

        IF target_publication_year > current_max_default_year
        THEN 
            UPDATE ggds_invdb.dim_layer
            SET default_year = target_publication_year;
        END IF;




        -- 3: dim_source_name
        LOCK TABLE ggds_invdb.dim_source_name IN EXCLUSIVE MODE;

        -- fetch the max source_name_id
        SELECT max(source_name_id) + 1 INTO next_id
        FROM ggds_invdb.dim_source_name;

        source_to_target_source_name_id_obj := '{}'; -- initialize an empty JSON object

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_source_name
            WHERE pub_year_id = source_pub_year_id
            ORDER BY source_name_id ASC
        ) LOOP
            -- map the current row's source_name_id to the sequence counters in the JSONB
            source_to_target_source_name_id_obj := source_to_target_source_name_id_obj || jsonb_build_object(current_record.source_name_id::TEXT, next_id);
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_source_name (source_name_id,
                                                    source_name,
                                                    sector_id,
                                                    layer_id,
                                                    category_id,
                                                    pub_year_id,
                                                    sub_category_1)
            VALUES  (next_id,
                    current_record.source_name,
                    current_record.sector_id,
                    current_record.layer_id,
                    current_record.category_id,
                    target_pub_year_id,
                    current_record.sub_category_1);

            next_id = next_id + 1; 
        END LOOP;




        -- 4: dim_report
        LOCK TABLE ggds_invdb.dim_report IN EXCLUSIVE MODE;

        -- fetch the max report_id
        SELECT max(report_id) + 1 INTO next_id
        FROM ggds_invdb.dim_report;

        source_to_target_report_id_obj := '{}'; -- initialize an empty JSON object

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_report
            WHERE reporting_year = source_publication_year
            ORDER BY report_id ASC
        ) LOOP
            -- map the current row's report_id to the sequence counters in the JSONB
            source_to_target_report_id_obj := source_to_target_report_id_obj || jsonb_build_object(current_record.report_id::TEXT, next_id);
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_report (report_id, --0
                                               report_name, --1
                                               created_date, --2
                                               created_by, --3
                                               last_updated_date, --4
                                               last_updated_by, --5
                                               report_title, --6
                                               report_rows_header, --7
                                               reporting_year, --8
                                               layer_id, --9
                                               report_refresh_date, --10
                                               refresh_status, --11
                                               tabs) --12
            VALUES (next_id,--0
                current_record.report_name, --1
                CURRENT_TIMESTAMP, --2
                input_user_id, --3
                CURRENT_TIMESTAMP, --4
                input_user_id, --5
                current_record.report_title, --6
                current_record.report_rows_header, --7
                target_publication_year, --8
                current_record.layer_id, --9
                NULL, --10
                NULL, --11
                current_record.tabs); --12

            next_id = next_id + 1; 
        END LOOP;




        -- 5: dim_report_row
        LOCK TABLE ggds_invdb.dim_report_row IN EXCLUSIVE MODE;

        -- fetch the max report_row_id
        SELECT max(report_row_id) + 1 INTO next_id
        FROM ggds_invdb.dim_report_row;

        source_to_target_report_row_id_obj := '{}'; -- initialize an empty JSON object

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_report_row
            WHERE report_id IN (
                SELECT jsonb_object_keys::INTEGER as old_report_id
                FROM jsonb_object_keys(source_to_target_report_id_obj)
            )
            ORDER BY report_row_id ASC
        ) LOOP
            -- map the current row's report_row_id to the sequence counters in the JSONB
            source_to_target_report_row_id_obj := source_to_target_report_row_id_obj || jsonb_build_object(current_record.report_row_id::TEXT, next_id);
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_report_row (report_row_id, --1
                                                   report_id, --2
                                                   row_group, --3
                                                   row_subgroup, --4
                                                   row_title, --5
                                                   totals_flag, --6
                                                   exclude_flag, --7
                                                   row_order, --8
                                                   query_formula_id, --9
                                                   query_formula_parameters, --10
                                                   created_date, --11
                                                   created_by, --12
                                                   last_updated_date, --13
                                                   last_updated_by, --14
                                                   ghg_id) --15
            VALUES (next_id, --1
                   (source_to_target_report_id_obj->(current_record.report_id::TEXT))::INTEGER, --2
                   current_record.row_group, --3
                   current_record.row_subgroup, --4
                   current_record.row_title, --5
                   current_record.totals_flag, --6
                   current_record.exclude_flag, --7
                   current_record.row_order, --8
                   current_record.query_formula_id, --9
                   current_record.query_formula_parameters, --10
                   CURRENT_TIMESTAMP, --11
                   input_user_id, --12
                   CURRENT_TIMESTAMP, --13
                   input_user_id, --14
                   current_record.ghg_id); --15
            
            next_id = next_id + 1; 
        END LOOP;




        -- 6: dim_emissionsQC_load_target
        LOCK TABLE ggds_invdb.dim_emissionsQC_load_target IN EXCLUSIVE MODE;

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_emissionsQC_load_target
            WHERE source_name_id IN (
                SELECT jsonb_object_keys::INTEGER as old_source_name_id
                FROM jsonb_object_keys(source_to_target_source_name_id_obj)
            )
            -- WHERE report_row_id IN (
            --     SELECT report_row_id 
            --     FROM ggds_invdb.dim_report_row
            --     WHERE report_id IN (
            --         SELECT report_id 
            --         FROM ggds_invdb.dim_report
            --         WHERE reporting_year = target_publication_year
            --     )
            -- )
            ORDER BY emissionsqc_load_target_id ASC
        ) LOOP
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_emissionsQC_load_target (source_name_id, --1
                                                                reporting_year, --2
                                                                layer_id, --3
                                                                target_tab, --4
                                                                row_title_cell, --5
                                                                anticipated_row_title, --6
                                                                data_ref_1990, --7
                                                                emission_parameters, --8
                                                                report_row_id, --9
                                                                created_date, --10
                                                                created_by, --11
                                                                last_updated_date, --12
                                                                last_updated_by) --13
            VALUES ((source_to_target_source_name_id_obj->(current_record.source_name_id::TEXT))::INTEGER,  --1
                    target_publication_year, --2
                    current_record.layer_id, --3
                    current_record.target_tab, --4
                    current_record.row_title_cell, --5
                    current_record.anticipated_row_title, --6
                    current_record.data_ref_1990, --7
                    current_record.emission_parameters, --8
                    (source_to_target_report_row_id_obj->(current_record.report_row_id::TEXT))::INTEGER, --9   //STILL NEEDS REMAPPING
                    CURRENT_TIMESTAMP, --10
                    input_user_id, --11
                    CURRENT_TIMESTAMP, --12
                    input_user_id); --13
        END LOOP;
        



        -- 7: dim_qc_report
        LOCK TABLE ggds_invdb.dim_qc_report IN EXCLUSIVE MODE;

        -- fetch the max qc_report_id
        SELECT max(qc_report_id) + 1 INTO next_id
        FROM ggds_invdb.dim_qc_report;

        source_to_target_qc_report_id_obj := '{}'; -- initialize an empty JSON object

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_qc_report
            WHERE reporting_year = source_publication_year
            ORDER BY qc_report_id ASC
        ) LOOP
            -- map the current row's qc_report_id to the sequence counters in the JSONB
            source_to_target_qc_report_id_obj := source_to_target_qc_report_id_obj || jsonb_build_object(current_record.qc_report_id::TEXT, next_id);
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_qc_report (qc_report_id, --0
                                                  qc_report_name, --1
                                                  qc_report_title, --2
                                                  qc_report_rows_header, --3
                                                  reporting_year, --4
                                                  layer_id, --5
                                                  report_refresh_date, --6
                                                  refresh_status, --7
                                                  created_date, --8
                                                  created_by, --9
                                                  last_updated_date, --10
                                                  last_updated_by) --11
            VALUES (next_id,--0
                    current_record.qc_report_name, --1
                    current_record.qc_report_title, --2
                    current_record.qc_report_rows_header, --3
                    target_publication_year, --4
                    current_record.layer_id, --5
                    NULL, --6
                    NULL, --7
                    CURRENT_TIMESTAMP, --8
                    input_user_id, --9
                    CURRENT_TIMESTAMP, --10
                    input_user_id); --11

            next_id = next_id + 1; 
        END LOOP;




        -- 8: dim_qc_comp_report_row
        LOCK TABLE ggds_invdb.dim_qc_comp_report_row IN EXCLUSIVE MODE;

        -- fetch the max qc_report_row_id
        SELECT max(qc_report_row_id) + 1 INTO next_id
        FROM ggds_invdb.dim_qc_comp_report_row;

        -- loop through the rows that should be duplicated (all belonging to source pub year)
        FOR current_record IN (
            SELECT * 
            FROM ggds_invdb.dim_qc_comp_report_row
            WHERE qc_report_id IN (
                SELECT jsonb_object_keys::INTEGER as old_qc_report_id
                FROM jsonb_object_keys(source_to_target_qc_report_id_obj)
            )
            ORDER BY qc_report_row_id ASC
        ) LOOP
            -- add a modified version of the current row to the table to stand in as the new year's table entry 
            INSERT INTO ggds_invdb.dim_qc_comp_report_row (qc_report_row_id, --1
                                                           qc_report_id, --2
                                                           row_group, --3
                                                           row_subgroup, --4
                                                           row_title, --5
                                                           totals_flag, --6
                                                           exclude_flag, --7
                                                           row_order, --8
                                                           emissions_query_formula_id, --9
                                                           emissions_query_formula_parameters, --10
                                                           qc_query_formula_id, --11
                                                           qc_query_formula_parameters, --12
                                                           created_date, --13
                                                           created_by, --14
                                                           last_updated_date, --15
                                                           last_updated_by) --16
            VALUES (next_id, --1
                   (source_to_target_qc_report_id_obj->(current_record.qc_report_id::TEXT))::INTEGER, --2
                   current_record.row_group, --3
                   current_record.row_subgroup, --4
                   current_record.row_title, --5
                   current_record.totals_flag, --6
                   current_record.exclude_flag, --7
                   current_record.row_order, --8
                   current_record.emissions_query_formula_id, --9
                   current_record.emissions_query_formula_parameters, --10
                   current_record.qc_query_formula_id, --11
                   current_record.qc_query_formula_parameters, --12
                   CURRENT_TIMESTAMP, --13
                   input_user_id, --14
                   CURRENT_TIMESTAMP, --15
                   input_user_id); --16
            
            next_id = next_id + 1; 
        END LOOP;




        -- 9: dim_excel_report
        INSERT INTO ggds_invdb.dim_excel_report (report_name,
                                                 "filename",
                                                 file_content,
                                                 last_created_date,
                                                 last_created_by,
                                                 file_type,
                                                 file_size,
                                                 reporting_year,
                                                 layer_id)
        SELECT t.report_name,
               t."filename",
               t.file_content,
               CURRENT_TIMESTAMP,
               input_user_id,
               t.file_type,
               t.file_size,
               target_publication_year,
               t.layer_id
        FROM ggds_invdb.dim_excel_report t
        WHERE reporting_year = source_publication_year;




        RETURN source_to_target_report_row_id_obj;
        RETURN TRUE;

    END
    $BODY$ LANGUAGE PLPGSQL;

    -- test with results    
    -- SELECT * FROM ggds_invdb.initialize_target_year(4, 13, 11);