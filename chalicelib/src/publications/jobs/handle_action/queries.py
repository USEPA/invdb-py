from chalicelib.src.reports.models.Report import Report
import chalicelib.src.database.methods as db_methods
import chalicelib.src.database.constants as db_constants
from chalicelib.src.reports.jobs.processing.methods import fetch_validated_reports
import chalicelib.src.publications.constants as pub_constants
import chalicelib.src.general.helpers as helpers
import json

pgdb_connection = db_methods.get_pgdb_connection()

def fetch_publication_data_product_info(pub_object_id: int, action: str) -> dict:
    """get relevant information about the requested publication data product: name of the script to run, pub_year, and layer_id"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT {"prepare_button_script" if action == db_constants.PUBLICATION_ACTIONS["PREPARE"] else "refine_button_script"} AS selected_script,
                              pub_ver.pub_year AS reporting_year, 
                              pub_ver.layer_id
                       FROM {db_constants.DB_TABLES["PUBLICATION_OBJECT"]} pub_obj
                            JOIN {db_constants.DB_TABLES["DIM_PUBLICATION"]} dim_pub ON pub_obj.pub_id = dim_pub.publication_id
                            JOIN {db_constants.DB_TABLES["PUBLICATION_VERSION"]} pub_ver ON pub_obj.pub_version_id = pub_ver.pub_version_id
                       WHERE pub_obj.pub_object_id = {pub_object_id}""")
    
    return cursor.fetchone()


def call_database_query_function(query_function_name: str, *params) -> list or None:
    """runs the passed query function in the database and returns the SQL response exactly as is."""
    cursor = pgdb_connection.cursor()
    helpers.tprint(f"The query function call is {query_function_name}{helpers.get_sql_list_str(params)}.")
    cursor.execute(f"""SELECT *
                       FROM {query_function_name}{helpers.get_sql_list_str(params)}""")
    results = cursor.fetchall()
    
    helpers.tprint(f"The query cameback with {len(results)} results.")
    
    if len(results) > 0: 
        helpers.tprint(f"The first result is: {results[0]}.")
    
    return results


def fetch_publication_refine_script_data(pub_object_id: int) -> list or None:
    """fetches the raw_data JSON value from the publication_object table of the database for the selected pub_object_id"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT raw_data
                       FROM {db_constants.DB_TABLES["PUBLICATION_OBJECT"]}
                       WHERE pub_object_id = {pub_object_id}""")
    results = cursor.fetchone()[0]
    if results is None: 
        raise ValueError("fetch_publication_refine_script_data(): there is no raw data found for this publication object. Be sure to run the 'prepare/import' script before proceeding with the 'refine/redact' script.")
    else: 
        results = json.loads(results)
    
    helpers.tprint(f"The query cameback with {len(results)} results.")
    
    if isinstance(results, list) and len(results) > 0:
        helpers.tprint(f"The first result is: {results[0]}")
    return results


def copy_publication_raw_data_to_refined(pub_object_id: int) -> list or None:
    """copies the data from raw_data column into the refined_data column for the row with the specified publication object"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""UPDATE {db_constants.DB_TABLES["PUBLICATION_OBJECT"]}
                       SET refined_data = raw_data 
                       WHERE pub_object_id = {pub_object_id}""")
    return False


def fetch_aggregated_ghg_chemicals(ghg_name_select="ghg_longname"):
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT ghg.{ghg_name_select}, ghg_category_code
                       FROM {db_constants.DB_TABLES["DIM_GHG_CATEGORY"]} ghg_cat
                           JOIN {db_constants.DB_TABLES["DIM_GHG"]} ghg ON ghg_cat.ghg_category_id = ghg.ghg_category_id
                       WHERE ghg.ghg_longname IN {helpers.get_sql_list_str(db_methods.fetch_redacted_ghg_chemicals())}""")
    results = cursor.fetchall()

    return {result[0]: result[1] for result in results}


def fetch_publication_raw_tablename(pub_object_id: int) -> dict:
    """get the raw data table name for the requested publication data product"""
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT raw_tablename
                       FROM {db_constants.DB_TABLES["PUBLICATION_OBJECT"]}
                       WHERE pub_object_id = {pub_object_id}""")
    
    return cursor.fetchone()[0]


def update_data_product_result_in_database(pub_object_id: int, action: str, results: [dict], table_name: str, user_id: int or None, skip_updating_data_cell: bool = False) -> None:
    """update the appropriate data, updated_date, and updated_by fields in the publication_object table based on if the action was to prepare or to refine"""
    tablename_column_name, data_column_name, date_updated_column_name, user_column_name = (("raw_tablename", "raw_data", "last_import_date", "last_import_by") if action == db_constants.PUBLICATION_ACTIONS["PREPARE"] else ("refined_tablename", "refined_data", "last_refined_date", "last_refined_by"))
    # tablename_column_name, data_column_name, date_updated_column_name, user_column_name = (("raw_tablename", "test_raw_data", "last_import_date", "last_import_by") if action == db_constants.PUBLICATION_ACTIONS["PREPARE"] else ("refined_tablename", "test_refined_data", "last_refined_date", "last_refined_by"))
    update_raw_total_records_sql_str = f", raw_total_records = {len(results)}" if action == db_constants.PUBLICATION_ACTIONS["PREPARE"] else ""
    helpers.tprint(f"The JSON came in the format: {str(json.dumps(results))[0]}")
    try:
        cursor = pgdb_connection.cursor()
        if skip_updating_data_cell:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["PUBLICATION_OBJECT"]}
                            SET {tablename_column_name} = '{table_name}', {date_updated_column_name} = CURRENT_TIMESTAMP, {user_column_name} = {user_id if isinstance(user_id,int) else "NULL"}{update_raw_total_records_sql_str}
                            WHERE pub_object_id = {pub_object_id}""")
        else:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["PUBLICATION_OBJECT"]}
                            SET {tablename_column_name} = '{table_name}', {data_column_name} = '{json.dumps(results).replace("'", "''")}', {date_updated_column_name} = CURRENT_TIMESTAMP, {user_column_name} = {user_id if isinstance(user_id,int) else "NULL"}{update_raw_total_records_sql_str}
                            WHERE pub_object_id = {pub_object_id}""")
        pgdb_connection.commit()

    except Exception as error:
        pgdb_connection.rollback()
        raise error from None