from chalicelib.src.reports.models.Report import *
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as globals
import chalicelib.src.reports.constants as report_constants
import psycopg2
import io

pgdb_connection = db_methods.get_pgdb_connection()

def fetch_validated_reports(reporting_year: int, layer_id: int, ids: [int]=None) -> [Report]:
    cursor = pgdb_connection.cursor()
    cursor.execute(f"""SELECT report_id,
                              created_by, 
                              content,
                              report_name
                       FROM {db_constants.DB_TABLES["REPORT"]}
                       WHERE {f'''reporting_year = {reporting_year} 
                             AND layer_id = {layer_id}''' if ids is None else f'report_id IN {helpers.get_sql_list_str(ids)}'}
                             AND validation_status = '{report_constants.VALIDATION_RESULTS['SUCCESS']}'""")    
    return cursor.fetchall()


def fetch_report_validation_error_rows(reports: [Report]) -> [(int, int)] or None: 
    """
    input:  reporting_year (type int): reporting year as found in the report table
            layer_id (type int): layer_id as found in the report table (1: National, 2: State)
    output: gives a list of tuples ([0]: report_id, [1]: row_number)
    """
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""SELECT DISTINCT report_id,
                            row_num::integer
            FROM    {db_constants.DB_TABLES["VALIDATION_LOG_REPORT"]}
            WHERE   report_id IN {helpers.get_sql_list_str([report.get_report_id() for report in reports])}"""
    )
    results = cursor.fetchall()
    if results is None:
        error_rows = {report.get_report_id(): [] for report in reports}
    else:
        # converts the list of tuples into a dictionary mapping each report ID to its list of error rows
        error_rows = {k: list(set(v for _, v in results if _ == k)) for k, _ in results}
        for id in [report.get_report_id() for report in reports]:
            if id not in error_rows.keys():
                error_rows.update({id: []})
    return error_rows

def process_report_query_batch(query_batch: [tuple], query_formula_info: dict, report_type, reporting_year: int, layer_id: int) -> [tuple]:
    """input: list of tuples. Each tuple represents a query formula call
    ([0]: row number within report, [1]: formula_prefix, [2:]: variable 
    number of arguments to pass to the query function.)
    output: dict where keys denote the result row number and map to the row 
    of values from 1990 to the max time series to write into Query_Results
    tab of the report"""
    # generate the SQL query statement
    pub_year_id = db_methods.fetch_pub_year_id(reporting_year)
    SQL_statement = ""
    for query in query_batch:
        mapped_function_name = query_formula_info[query[1]]
        argument_list_str = ", ".join([f"'{arg}'" for arg in query[2:]])
        SQL_statement += f"""SELECT {query[0]} AS row, *
        FROM ggds_invdb.{mapped_function_name}{helpers.get_sql_list_str([pub_year_id, layer_id] + list(query[2:]))}
        UNION ALL\n"""
    SQL_statement = SQL_statement[:-10] # strip the trailing 'UNION ALL\n'
    SQL_statement += f"ORDER BY row,{' geo_ref, ' if report_type == report_constants.REPORT_TYPES['STATE'] else ' '} year;"

    cursor = pgdb_connection.cursor()
    cursor.execute(SQL_statement)
    results = cursor.fetchall()
    width = pub_year_id + 22 # width of the time series
    
    return results, width


def batch_update_report_content_in_database(reports: [Report]) -> None:
    """updates the content field of the database with the file contents including the processed query results.
    Only applies to reports that could process to completion."""
    # Construct the UPDATE query
    reports_thats_processed_successfully = [rep for rep in reports if rep.process_result == "SUCCESS"]
    if len(reports_thats_processed_successfully) == 0:
        return 
        
    update_query = f"""
        UPDATE {db_constants.DB_TABLES["REPORT"]}
        SET content = 
            CASE 
    """
    for report in reports_thats_processed_successfully:
        with report as opened_report:
            opened_report.switch_to_results_tab()
            bytes_io = io.BytesIO()
            opened_report.save_to(bytes_io)
            file_contents = bytes_io.getvalue()
            update_query += f"WHEN report_id = {opened_report.get_report_id()} THEN {psycopg2.Binary(file_contents)}\n"
    update_query += f"""
            END
        WHERE report_id IN {helpers.get_sql_list_str([report.get_report_id() for report in reports_thats_processed_successfully])}
    """
    try:
        cursor = pgdb_connection.cursor()
        cursor.execute(update_query)
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_report_processing_dates_in_database(reports: [Report], user_id: int):
    reports_thats_processed_successfully = [report.get_report_id() for report in reports if report.process_result == "SUCCESS"]
    if len(reports_thats_processed_successfully) == 0:
        return
    try:
        cursor = pgdb_connection.cursor()
        cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                        SET processed_date = CURRENT_TIMESTAMP, processed_by = {user_id}, last_updated_date = CURRENT_TIMESTAMP, last_updated_by = {user_id if isinstance(user_id,int) else "NULL"}
                        WHERE report_id IN {helpers.get_sql_list_str(reports_thats_processed_successfully)}""")
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None