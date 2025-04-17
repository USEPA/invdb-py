import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
from chalicelib.src.source_files.models.SourceFile import SourceFile
from chalicelib.src.source_files.models.QCLoadTarget import QCLoadTarget
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.general.globals as globals
import json
import os

pgdb_connection = db_methods.get_pgdb_connection()


def fetch_source_files_by_id(source_name_ids: [int], reporting_year: int, layer_id: int) -> list[SourceFile]:
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""WITH 
        source_name_ids AS (
        SELECT
            unnest({helpers.get_sql_array_str(source_name_ids)}) as source_name_id
        ),
        max_dates AS (
            SELECT
                source_file_id,
                MAX(last_srcfile_linked_date) AS max_date
            FROM
                {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]}
            GROUP BY
                source_file_id
        )
        SELECT
            a.attachment_id,
            a.content,
            s.source_file_id,
            a.attachment_name,
            s.source_name_id
        FROM
            {db_constants.DB_TABLES["SOURCE_FILE"]} s
        JOIN
            {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]} a ON s.source_file_id = a.source_file_id
        JOIN 
            source_name_ids sni ON sni.source_name_id = s.source_name_id
        JOIN
            max_dates md ON a.source_file_id = md.source_file_id
        WHERE
            a.last_srcfile_linked_date = md.max_date
            AND s.reporting_year = {reporting_year}
            AND s.layer_id = {layer_id}
            AND s.validation_status = 'success'
            {"AND a.processed_date is NULL" if not globals.debug else ""}
            AND NOT s.is_deleted"""
    )
    
    source_files = []
    max_time_series = db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    for source_file in cursor.fetchall():
        filename, extension = os.path.splitext(source_file[3])

        new_source_file = SourceFile(
            source_file[2],
            source_file[0],
            source_file[1],
            max_time_series,
            reporting_year,
            created_by=None,
            read_only=True,
            source_file_name=source_file[3],
            extension=extension
        )
        new_source_file.set_source_name_id(source_file[4])
        new_source_file.set_layer_id(layer_id)
        source_files.append(new_source_file)
    return source_files

def fetch_qc_load_targets_by_id(source_name_id: int, reporting_year: int) -> list[QCLoadTarget]:
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""
        SELECT
            d.emissionsqc_load_target_id,
            d.source_name_id,
            d.reporting_year,
            d.layer_id,
            d.target_tab,
            d.row_title_cell,
            d.anticipated_row_title,
            d.data_ref_1990,
            d.emission_parameters,
            d.report_row_id
        FROM {db_constants.DB_TABLES["DIM_EMISSIONSQC_LOAD_TARGET"]} d
        WHERE d.source_name_id = {source_name_id} and d.reporting_year = {reporting_year}
        """
    )
    load_targets = []
    for load_target in cursor.fetchall():
        load_targets.append(
            QCLoadTarget(
                load_target[0],
                load_target[1],
                load_target[2],
                load_target[3],
                load_target[4],
                load_target[5],
                load_target[6],
                load_target[7],
                load_target[8],
                load_target[9],
            )
        )

    return load_targets


def fetch_emissionsqc_key_data_type_ids():
    """returns a list of all the data type ids that have the target table of 'emissionsqc_key' and is active"""
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""SELECT data_type_id 
            FROM {db_constants.DB_TABLES['DIM_DATA_TYPE']} 
            WHERE   target_table = '{db_constants.DB_TABLES['EMISSIONSQC_KEY'].split(".")[1]}'
                    AND active = True"""
    )
    return [value[0] for value in cursor.fetchall()]

def update_qc_facts_archive_table(facts: list) -> None:
    """removes pre-existing facts_archive records for input source files and adds new facts_archive records gathered by the current source file load request"""
    cursor = pgdb_connection.cursor()

    if len(facts) == 0:
        helpers.tprint("No QC data for facts_archive, skipping")
        return

    # fetch the pub_year_dim and time_series_dim values
    cursor.execute(
        f"""
        SELECT max_time_series, pub_year_id 
        FROM {db_constants.DB_TABLES["DIM_PUBLICATION_YEAR"]}"""
    )
    pub_year_map = {row[0]: row[1] for row in cursor.fetchall()} # { max_time_series: pub_year_id }
    cursor.execute(
        f"""
        SELECT year, year_id
        FROM {db_constants.DB_TABLES["DIM_TIME_SERIES"]}"""
    )
    time_series_id_mappings = {mapping[0]: mapping[1] for mapping in cursor.fetchall()}

    try:
        source_file_id = facts[0]["source_file_id"]

        # delete the existing fact_archive data that corresponds with incoming data (same emissionsqc_key, reporting_year, and layer_id)
        # calls a query function from the database to handle this
        cursor.execute(
            f"""SELECT * 
                FROM {db_constants.DB_QUERY_FUNCTIONS["DELETE_EMISSIONS_QC_FACTS_ARCHIVE"]}({helpers.get_sql_array_str([source_file_id])})"""
        )

        # making sure to remove duplicates before updating
        # unique_hashes = tuple(set([fact["hash"] for fact in facts]))
        # emissionqc_data_type_ids = tuple(fetch_emissionsqc_key_data_type_ids())
        # if len(unique_hashes) > 0 and len(emissionqc_data_type_ids) > 0:
        #     del_facts_query = (f"delete from {db_constants.DB_TABLES['FACTS_ARCHIVE']} where data_type_id in {helpers.get_sql_list_str(emissionqc_data_type_ids)} and key_id in {helpers.get_sql_list_str(unique_hashes)}")
        #     del_key_query = (f"delete from {db_constants.DB_TABLES['EMISSIONSQC_KEY']} where emissionsqc_uid in {helpers.get_sql_list_str(unique_hashes)}")
        #     cursor.execute(del_facts_query)
        #     cursor.execute(del_key_query)

        # insert the new facts_archive data
        values = []
        for fact in facts:
            data_type_id = fact["data_type_id"]
            key_id = fact["hash"]
            layer_id = fact["layer_id"]
            pub_year_id = pub_year_map[fact["time_series"]]
            attachment_id = fact["attachment_id"]

            for index, year_value in enumerate(fact["years"]):
                year_id = time_series_id_mappings[qc_constants.EARLIEST_REPORTING_YEAR + index]
                if attachment_id == 1010:
                    print("qc extraction inserting row ", f"({data_type_id}, '{key_id}'::uuid, {layer_id}, {pub_year_id}, {year_id}, '{str(year_value)}', {attachment_id})")
                values.append(f"({data_type_id}, '{key_id}'::uuid, {layer_id}, {pub_year_id}, {year_id}, '{str(year_value)}', {attachment_id})")

        values_string = ', '.join(values)

        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES['FACTS_ARCHIVE']} VALUES {values_string}"""
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def update_qc_validation_error_logs(errors: list, source_name_ids: [int], reporting_year: int, layer_id: int, user_id: int) -> None:
    cursor = pgdb_connection.cursor()

    

    # insert the new error data
    values = []
    for error in errors:
        attachment_id = error["attachment_id"]
        emissionsqc_load_target_id = error["load_target_id"]
        cell_value = error["cell_value"] if len(error["cell_value"]) < 250 else error["cell_value"][0:247] + "..."
        cell_location = error["cell_location"] if len(error["cell_location"]) < 50 else error["cell_location"][0:47] + "..."
        description = error["description"] if len(error["description"]) < 500 else error["description"][0:497] + "..."
        error_type = error["error_type"]
        created_date = 'LOCALTIMESTAMP(6)'
        created_user_id = user_id if user_id is not None else 'NULL'

        values.append(f"({attachment_id}, {emissionsqc_load_target_id}, '{cell_value}', '{cell_location}', '{description}', {error_type}, {created_date}, {created_user_id})")

    values_string = ', '.join(values)

    try:
        # first delete any pre-existing entries to avoid duplicates
        helpers.tprint(f"Removing existing validation_log_extract errors before inserting new ones...")
        cursor.execute(
            f"""WITH existing_ids as (
	                SELECT delt.emissionsqc_load_target_id 
                    FROM {db_constants.DB_TABLES['DIM_EMISSIONSQC_LOAD_TARGET']} delt 
                    WHERE delt.source_name_id IN {helpers.get_sql_list_str(source_name_ids)} 
                    AND delt.reporting_year = {reporting_year} 
                    AND delt.layer_id = {layer_id}
                ) 
                DELETE FROM {db_constants.DB_TABLES['VALIDATION_LOG_EXTRACT']} vle 
                WHERE vle.emissionsqc_load_target_id IN (SELECT * FROM existing_ids)"""
        )

        if len(errors) == 0:
            helpers.tprint("No QC validation errors to log.")
            return

        # then insert new errors
        helpers.tprint(f"Updating validation_log_extract errors: {values_string}")
        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES['VALIDATION_LOG_EXTRACT']} (attachment_id, emissionsqc_load_target_id, cell_value, cell_location, description, error_type, created_date, created_user_id) VALUES {values_string}"""
        )

        # update error status of attachments to reflect any new errors
        cursor.execute(
            f"""WITH qc_error_ids AS ( 
                    SELECT vle.attachment_id 
                    FROM {db_constants.DB_TABLES['VALIDATION_LOG_EXTRACT']} vle 
                ) 
                UPDATE {db_constants.DB_TABLES['SOURCE_FILE_ATTACHMENT']} 
                SET has_error = true 
                WHERE attachment_id IN (SELECT * FROM qc_error_ids)"""
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def update_emissionsqc_key_table(keys: list) -> None:
    """insert new emissionsqc keys into the emissionsqc_keys table, skip inserting any emissionsqc keys that are already present"""
    cursor = pgdb_connection.cursor()

    if len(keys) == 0:
        helpers.tprint("No QC data for emissionsqc_key, skipping")
        return

    existing_hashes = set()

    # List comprehension to filter out duplicates
    filtered_keys = [d for d in keys if d["hash"] not in existing_hashes and not existing_hashes.add(d["hash"])]

    emissionsqc_key_data = filtered_keys

    # fetch all emissions_uids from the database so we can check for duplicates in input
    cursor.execute(
        f"""SELECT emissionsqc_uid FROM {db_constants.DB_TABLES['EMISSIONSQC_KEY']}"""
    )
    results = cursor.fetchall()
    preexisting_emissions_uids = {
        row[0].replace("-", ""): None for row in results
    }  # place the uids in a dict for fast retrieval time

    # remove any emission keys from the source file input that are already present in the database
    i = 0
    while i < len(emissionsqc_key_data):
        if emissionsqc_key_data[i]["hash"] in preexisting_emissions_uids:
            del emissionsqc_key_data[i]
            i -= 1
        i += 1

    # if there are no NEW emissions keys, exit the function
    if len(emissionsqc_key_data) == 0:
        return

    # build the 'INSERT' SQL command from the emissions_key_data
    values = []

    for row in emissionsqc_key_data:
        emissionsqc_uid = row['hash']
        parameters_json = json.dumps(row['parameters'])
        emissionsqc_load_targets_id = row['load_target_id']
        source_file_id = row['source_file_id']
        values.append(f"('{emissionsqc_uid}'::uuid, '{parameters_json}', {emissionsqc_load_targets_id}, {source_file_id})")

    values_string = ', '.join(values)

    try:
        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES["EMISSIONSQC_KEY"]} (emissionsqc_uid, parameters, emissionsqc_load_targets_id, source_file_id)
                VALUES
                {values_string}
            """
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None
