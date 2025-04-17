import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
from chalicelib.src.source_files.models.SourceFile import SourceFile
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.general.globals as globals
import os

pgdb_connection = db_methods.get_pgdb_connection()


def fetch_unarchived_source_files(reporting_year, layer_id, ids: list[int]=None) -> list[SourceFile]:
    """
    input:  reporting_year (type int): reporting year as found in the source_file table
            layer_id (type int): layer_id as found in the source_file table (1: National, 2: State)
    output: gives a list of triples ([0]: attachment_id, [1]: content, [2]: source_file_id)
    """
    cursor = pgdb_connection.cursor()
    print("debug is ", globals.debug, ", ids is ", bool(ids))
    cursor.execute(
        f"""WITH max_dates AS (
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
                max_dates md ON a.source_file_id = md.source_file_id
            WHERE
                s.layer_id = {layer_id}
                AND s.reporting_year = {reporting_year}
                {'''AND a.last_srcfile_linked_date = md.max_date
                    AND s.validation_status = 'success'
                    AND a.processed_date is NULL'''
                if not (ids and globals.debug) 
                else f"AND a.attachment_id IN {helpers.get_sql_list_str(ids)}"}
                AND NOT s.is_deleted"""
    )
    max_time_series = db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    source_files = []
    for source_file in cursor.fetchall():
        filename, extension = os.path.splitext(source_file[3])

        new_source_file = SourceFile(
            source_file[2],
            source_file[0],
            source_file[1],
            max_time_series,
            reporting_year,
            created_by=None,
            read_only=False,
            source_file_name=source_file[3],
            extension=extension,
            layer_id=layer_id
        )
        new_source_file.set_source_name_id(source_file[4])
        source_files.append(new_source_file)
    return source_files


def fetch_source_file_validation_error_rows(reporting_year, layer_id):
    """
    input:  reporting_year (type int): reporting year as found in the source_file table
            layer_id (type int): layer_id as found in the source_file table (1: National, 2: State)
    output: gives a list of tuples ([0]: attachment_id, [1]: row_number)
    """
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""WITH max_dates AS (
                SELECT
                    source_file_id,
                    MAX(last_srcfile_linked_date) AS max_date
                FROM
                    {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]}
                GROUP BY
                    source_file_id
            )
            SELECT DISTINCT
                a.attachment_id,
                v.row_number::integer
            FROM
                {db_constants.DB_TABLES["SOURCE_FILE"]} s
            JOIN
                {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]} a ON s.source_file_id = a.source_file_id
            JOIN
                max_dates md ON a.source_file_id = md.source_file_id
            JOIN 
                {db_constants.DB_TABLES["VALIDATION_LOG_LOAD"]} v ON a.attachment_id = v.attachment_id
            WHERE
                s.layer_id = {layer_id}
                AND s.reporting_year = {reporting_year}
                AND a.last_srcfile_linked_date = md.max_date"""
    )
    return cursor.fetchall()


def fetch_dim_table_foreign_keys():
    """
    output: foreign_keys: dict ->   key: column name,
                                value: dict ->  key: column value
                                                value: foreign key
    """
    queries = {
        "data_type": f"""SELECT data_type_name, data_type_id FROM {db_constants.DB_TABLES["DIM_DATA_TYPE"]}""",
        "sector": f"""SELECT sector_name, sector_id FROM {db_constants.DB_TABLES["DIM_SECTOR"]}""",
        "subsector": f"""SELECT subsector_name, subsector_id FROM {db_constants.DB_TABLES["DIM_SUBSECTOR"]}""",
        "category": f"""SELECT category_name, category_id FROM {db_constants.DB_TABLES["DIM_CATEGORY"]}""",
        "fuel": f"""SELECT fuel_type_name, fuel_type_id FROM {db_constants.DB_TABLES["DIM_FUEL_TYPE"]}""",
        "ghg": f"""SELECT ghg_code, ghg_longname, ghg_shortname, cas_no, ghg_id FROM {db_constants.DB_TABLES["DIM_GHG"]}""",
    }

    results = {}
    with pgdb_connection.cursor() as cursor:
        for query in queries:
            cursor.execute(queries[query])
            result = cursor.fetchall()
            results[query] = result
    return results


def update_facts_archive_table(emissions_quantity_data: list, reporting_year: int, layer_id: int, source_file_ids: [int]) -> None:
    """removes pre-existing facts_archive records for input source files and adds new facts_archive records gathered by the current source file load request"""
    cursor = pgdb_connection.cursor()

    # fetch the pub_year_dim and time_series_dim values
    cursor.execute(
        f"""
        SELECT pub_year_id 
        FROM {db_constants.DB_TABLES["DIM_PUBLICATION_YEAR"]}
        WHERE pub_year = {reporting_year}"""
    )
    pub_year_id = cursor.fetchall()[0][0]
    cursor.execute(
        f"""
        SELECT year, year_id
        FROM {db_constants.DB_TABLES["DIM_TIME_SERIES"]}"""
    )
    time_series_id_mappings = {mapping[0]: mapping[1] for mapping in cursor.fetchall()}

    try:
        # delete the existing facts_archive data that corresponds with incoming data (same emissions_key, reporting_year, and layer_id)
        # calls a query function from the database to handle this
        if len(source_file_ids) > 0:
            cursor.execute(
                f"""SELECT * 
                    FROM {db_constants.DB_QUERY_FUNCTIONS["DELETE_EMISSIONS_FACTS_ARCHIVE"]}({helpers.get_sql_array_str(source_file_ids)})"""
            )
            cursor.execute(
                f"""SELECT * 
                    FROM {db_constants.DB_QUERY_FUNCTIONS["DELETE_ACTIVITY_FACTS_ARCHIVE"]}({helpers.get_sql_array_str(source_file_ids)})"""
            )
        
        # making sure to remove duplicates before updating
        # emission_data_type_ids = tuple(fetch_emissions_key_data_type_ids())
        # unique_hashes = tuple(set([obj[0][0] for obj in emissions_quantity_data]))
        # if len(unique_hashes) > 0 and len(emission_data_type_ids) > 0:
        #     del_facts_query = (f"""DELETE FROM {db_constants.DB_TABLES['FACTS_ARCHIVE']} 
        #                            WHERE layer_id = {layer_id} 
        #                                  AND data_type_id IN {helpers.get_sql_list_str(emission_data_type_ids)} 
        #                                  AND key_id IN {helpers.get_sql_list_str(unique_hashes)}""")
        #     del_key_query = (f"""DELETE FROM {db_constants.DB_TABLES['EMISSIONS_KEY']} ek
        #                          USING {db_constants.DB_TABLES['FACTS_ARCHIVE']} fa
        #                          WHERE ek.emissions_uid = fa.key_id 
        #                                AND fa.layer_id = {layer_id} 
        #                                AND ek.emissions_uid IN {helpers.get_sql_list_str(unique_hashes)}""")
        #     # probably pointless since 'using facts_archive', but just deleted those facts_archive records, but db equivalent already does this anyway
        #     del_akey_query = (f"""DELETE FROM {db_constants.DB_TABLES['ACTIVITY_KEY']} ak
        #                          USING {db_constants.DB_TABLES['FACTS_ARCHIVE']} fa
        #                          WHERE ak.emissions_uid = fa.key_id 
        #                                AND fa.layer_id = {layer_id} 
        #                                AND ak.emissions_uid IN {helpers.get_sql_list_str(unique_hashes)}""")
        #     cursor.execute(del_facts_query)
        #     cursor.execute(del_key_query)
        #     cursor.execute(del_akey_query)

        # insert the new facts_archive data
        values_string = ""
        for data_row in emissions_quantity_data:
            for quantity, i in zip(data_row[1], range(len(data_row[1]))):
                if {data_row[0][2]} == 1010:
                    print("emissions extraction inserting row ", f"""({data_row[0][1]}, '{data_row[0][0]}', {layer_id}, {pub_year_id}, {time_series_id_mappings[i + qc_constants.EARLIEST_REPORTING_YEAR]}, {f"'{quantity}'" if isinstance(quantity, str) else quantity}, '{data_row[0][2]}'), """)
                values_string += f"""({data_row[0][1]}, '{data_row[0][0]}', {layer_id}, {pub_year_id}, {time_series_id_mappings[i + qc_constants.EARLIEST_REPORTING_YEAR]}, {f"'{quantity}'" if isinstance(quantity, str) else quantity}, '{data_row[0][2]}'), """
        values_string = values_string[:-2]

        if len(values_string) > 0:
            cursor.execute(
                f"""INSERT INTO {db_constants.DB_TABLES['FACTS_ARCHIVE']} VALUES {values_string}"""
            )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def fetch_emissions_key_data_type_ids():
    """returns a list of all the data type ids that have the target table of 'emissions_key' and is active"""
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""SELECT data_type_id 
            FROM {db_constants.DB_TABLES['DIM_DATA_TYPE']} 
            WHERE   target_table = '{db_constants.DB_TABLES['EMISSIONS_KEY'].split(".")[1]}'
                    AND active = True"""
    )
    return [value[0] for value in cursor.fetchall()]


def fetch_activity_key_data_type_ids():
    """returns a list of all the data type ids that have the target table of 'activity_key' and is active"""
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""SELECT data_type_id 
            FROM {db_constants.DB_TABLES['DIM_DATA_TYPE']} 
            WHERE   target_table = '{db_constants.DB_TABLES['ACTIVITY_KEY'].split(".")[1]}'
                    AND active = True"""
    )
    return [value[0] for value in cursor.fetchall()]


def update_activity_key_table(activity_key_data: list, template: int) -> None:
    """insert new activity keys into the activity keys data into the activity_keys table, skip inserting any activity keys that are already present"""
    cursor = pgdb_connection.cursor()

    # remove duplicate value rows from the source_file input
    activity_key_data = list(set(activity_key_data))
    # fetch all activity_uids from the database so we can check for duplicates in input
    cursor.execute(
        f"""SELECT emissions_uid FROM {db_constants.DB_TABLES['ACTIVITY_KEY']}"""
    )
    results = cursor.fetchall()
    preexisting_emissions_uids = {
        row[0].replace("-", ""): None for row in results
    }  # place the uids in a dict for fast retrieval time

    # remove any activity keys from the source file input that are already present in the database
    i = 0
    while i < len(activity_key_data):
        if activity_key_data[i][1] in preexisting_emissions_uids:
            del activity_key_data[i]
            i -= 1
        i += 1

    # if there are no NEW activity keys, exit the function
    if len(activity_key_data) == 0:
        helpers.tprint("No new activity keys, skipping")
        return

    # build the 'INSERT' SQL command from the activity_key_data
    values_string = ""
    for activity_key_row in activity_key_data:
        temp_list = list(activity_key_row)[1:]
        temp_list[0] = f"'{temp_list[0]}'::uuid"
        values_string = values_string + "("
        for i in range(len(temp_list)):
            if isinstance(temp_list[i], str):
                temp_list[i] = temp_list[i].strip()
            if temp_list[i] in (None, ""):
                temp_list[i] = "NULL"
            if (
                isinstance(temp_list[i], str) and temp_list[i] != "NULL" and i != 0
            ):  # add string type values with single quotes
                values_string += f"'{str(temp_list[i])}', "
            else:
                values_string += f"{str(temp_list[i])}, "

        values_string = values_string[:-2] + "),\n"
    values_string = values_string[:-2]
    try:
        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES["ACTIVITY_KEY"]} 
                (emissions_uid, sector_id, sub_sector_id, category_id, sub_category_1, sub_category_2, 
                sub_category_3, sub_category_4, sub_category_5, carbon_pool, fuel_type_id_1, 
                fuel_type_id_2, geo_ref, "EXCLUDE", crt_code, id, cbi_activity, units, 
                {"ghg_category, " if template == 3 else ""}
                ghg_id, gwp, source_file_id)
                VALUES
                {values_string}
            """
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def update_emissions_key_table(emissions_key_data: list, template: int) -> None:
    """insert new emissions keys into the emissions keys data into the emissions_keys table, skip inserting any emissions keys that are already present"""
    cursor = pgdb_connection.cursor()

    # remove duplicate value rows from the source_file input
    emissions_key_data = list(set(emissions_key_data))
    # fetch all emissions_uids from the database so we can check for duplicates in input
    cursor.execute(
        f"""SELECT emissions_uid FROM {db_constants.DB_TABLES['EMISSIONS_KEY']}"""
    )
    results = cursor.fetchall()
    preexisting_emissions_uids = {
        row[0].replace("-", ""): None for row in results
    }  # place the uids in a dict for fast retrieval time

    # remove any emission keys from the source file input that are already present in the database
    i = 0
    while i < len(emissions_key_data):
        if emissions_key_data[i][1] in preexisting_emissions_uids:
            del emissions_key_data[i]
            i -= 1
        i += 1

    # if there are no NEW emissions keys, exit the function
    if len(emissions_key_data) == 0:
        return

    # build the 'INSERT' SQL command from the emissions_key_data
    values_string = ""
    for emissions_key_row in emissions_key_data:
        temp_list = list(emissions_key_row)[1:]
        temp_list[0] = f"'{temp_list[0]}'::uuid"
        values_string = values_string + "("
        for i in range(len(temp_list)):
            if isinstance(temp_list[i], str):
                temp_list[i] = temp_list[i].strip()
            if temp_list[i] in (None, ""):
                temp_list[i] = "NULL"
            if (
                isinstance(temp_list[i], str) and temp_list[i] != "NULL" and i != 0
            ):  # add string type values with single quotes
                values_string += f"'{str(temp_list[i])}', "
            else:
                values_string += f"{str(temp_list[i])}, "

        values_string = values_string[:-2] + "),\n"
    values_string = values_string[:-2]
    try:
        cursor.execute(
            f"""INSERT INTO {db_constants.DB_TABLES["EMISSIONS_KEY"]} 
                (emissions_uid, sector_id, sub_sector_id, category_id, sub_category_1, sub_category_2, 
                sub_category_3, sub_category_4, sub_category_5, carbon_pool, fuel_type_id_1, 
                fuel_type_id_2, geo_ref, "EXCLUDE", crt_code, id, cbi_activity, units, 
                {"ghg_category, " if template == 3 else ""}
                ghg_id, gwp, source_file_id)
                VALUES
                {values_string}
            """
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def update_source_file_processed_dates(attachment_ids: [int], user_id: int) -> None:
    cursor = pgdb_connection.cursor()
    try:
        cursor.execute(
            f"""UPDATE {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]}
                SET processed_date = CURRENT_TIMESTAMP, processed_by = {user_id}
                WHERE attachment_id IN {helpers.get_sql_list_str(attachment_ids)}"""
        )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None
    
def update_refresh_status_rollup_table(reporting_year: int, layer_id: int):
    # set refresh_status of all rollups in given layer and year to 'In Progress'
	# update if status record exist in table else insert
    query = f"""INSERT INTO {db_constants.DB_TABLES["REFRESH_STATUS_ROLLUP_TABLE"]}(pub_year_id, layer_id, refresh_status, last_update_date)
	 values(%s, %s, 'In Progress', CURRENT_TIMESTAMP)
	 ON CONFLICT (pub_year_id, layer_id) 
	 DO UPDATE 
	  SET refresh_status = 'In Progress', last_update_date = CURRENT_TIMESTAMP;"""
    db_methods.perform_query_update(query, db_methods.fetch_pub_year_id(reporting_year), layer_id)

def update_emissions_rollup_tables(reporting_year: int, layer_id: int):
    db_methods.perform_query_function(db_constants.DB_FUNCTIONS["F_REFRESH_ROLLUP_TABLE"], db_methods.fetch_pub_year_id(reporting_year), layer_id)