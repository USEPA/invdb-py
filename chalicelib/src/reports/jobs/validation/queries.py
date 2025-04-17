import chalicelib.src.reports.constants as report_constants
from chalicelib.src.reports.models.Report import *
from chalicelib.src.reports.models.ReportFactory import *
from chalicelib.src.database.methods import *
import chalicelib.src.database.constants as db_constants
from chalicelib.src.general.models.BatchValidationReport import *
import chalicelib.src.general.globals as globals


def fetch_unvalidated_reports(reporting_year: int, layer_id: int, ids: [int]=None) -> [Report]:
    """
    input:  reporting_year (type int): reporting year as found in the source_file table
            layer_id (type int): layer_id as found in the source_file table (1: National, 2: State)
    output: gives a list of quadruples ([0]: attachment_id, [1]: created_by, [2]: content, [3] source_file_id)
    """
    # formulate and send an SQL query to select the right reports from the database
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"""SELECT
                report_id,
                created_by, 
                content,
                report_name
            FROM
                {db_constants.DB_TABLES["REPORT"]}
            WHERE
                {f'''layer_id = {layer_id}
                AND reporting_year = {reporting_year}''' if ids is None else f'''report_id IN {helpers.get_sql_list_str(ids)}'''}
                {"" if globals.debug else "AND validation_status IS NULL"}"""
    )

    # get the response data
    results = cursor.fetchall()
    max_time_series = fetch_max_time_series_by_reporting_year(reporting_year) # done here to avoid multiple hits to the database
    
    # send the binary of each report to the report factory to determine the report type
    reports = []
    report_factory = ReportFactory()
    for report in results:
        reports.append(
            report_factory.get_report_from_factory(
                report[0],
                report[2],
                max_time_series,
                created_by=report[1],
                read_only=True,
                report_name=report[3],
            )
        )
    return reports


def fetch_query_formulas_info() -> dict:
    cursor = pgdb_connection.cursor()

    cursor.execute(f"""SELECT formula_prefix, parameters
                       FROM {db_constants.DB_TABLES["DIM_QUERY_FORMULA"]}""")
    
    results = cursor.fetchall()

    # return as a dict where the formula prefix is the key and parameter list are the value
    return {result[0].lower(): (result[1].split(", ") if isinstance(result[1], str) else result[1]) for result in results}


def batch_update_validation_log_report_table(batch_validation_report: BatchValidationReport):
    """updates the error logs for the all files"""
    new_validation_logs = batch_validation_report.generate_error_list()

    # delete the old validation logs from the validation_log_report table for the same national reports
    cursor = pgdb_connection.cursor()
    cursor.execute(
        f"DELETE FROM {db_constants.DB_TABLES['VALIDATION_LOG_REPORT']} WHERE report_id IN ({str(batch_validation_report.reports.keys())[11:-2]}) "
    )

    # add the new validation error logs if there are any
    try:
        if len(new_validation_logs) > 0:
            # create the SQL literal string for all the new rows
            values_string = ", ".join(
                [f"""({error[0]}, '{error[6]}', {error[3] if error[3] else 'NULL'}, '{error[1]}', 
                '{error[2].replace("'","''") if not (isinstance(error[2], str) and len(error[2]) >= 250) else f"{error[2][0:247]}...".replace("'","''")}', 
                '{error[4]}', LOCALTIMESTAMP(6), {error[5] if error[5] is not None else 'NULL'})""" for error in new_validation_logs]
            )
            # insert the new rows into the validation log load table
            cursor.execute(
                f"""INSERT INTO {db_constants.DB_TABLES['VALIDATION_LOG_REPORT']} (report_id, tab_name, row_num, 
                                                                                   field_name, value, description, 
                                                                                   created_date, created_by)
                    VALUES {values_string};"""
            )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None
    return len(new_validation_logs) > 0  # return whether there were errors or not


def batch_update_report_has_errors_flags(batch_validation_report: BatchValidationReport) -> None:
    """update the has_errors column of the report table in the database based on
    which report within the input batch validation report had errors"""
    report_ids_with_errors = [validation_report.get_report_id() for validation_report in batch_validation_report.get_validation_reports_with_errors()]
    report_ids_without_errors = [validation_report.get_report_id() for validation_report in batch_validation_report.get_validation_reports_without_errors()]

    cursor = pgdb_connection.cursor()
    try:
        if len(report_ids_with_errors) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['REPORT']} SET has_error = true WHERE report_id IN ({str(report_ids_with_errors)[1:-1]}) "
            )
        if len(report_ids_without_errors) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['REPORT']} SET has_error = false WHERE report_id IN ({str(report_ids_without_errors)[1:-1]}) "
            )
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_validation_status_in_database(batch_validation_report: BatchValidationReport) -> None:
    """update the validation_status column of the report table in the database based on
    which reports were able to validate to completion without throwing a fatal error"""
    try:
        cursor = pgdb_connection.cursor()
        report_ids_that_validated_successfully = [report.get_report_id() 
                                                    for report in batch_validation_report 
                                                    if report.get_validation_result() == report_constants.VALIDATION_RESULTS["SUCCESS"]
                                                 ]
        if len(report_ids_that_validated_successfully) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET validation_status = 'success'
                            WHERE report_id IN {helpers.get_sql_list_str(report_ids_that_validated_successfully)}""")
        
        report_ids_that_failed_to_validate = [report.get_report_id() 
                                                for report in batch_validation_report 
                                                if report.get_validation_result() == report_constants.VALIDATION_RESULTS["FAILED"]
                                             ]
        if len(report_ids_that_failed_to_validate) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET validation_status = 'failed'
                            WHERE report_id IN {helpers.get_sql_list_str(report_ids_that_failed_to_validate)}""")
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_report_types_in_database(batch_validation_report: BatchValidationReport) -> None:
    """update the report_type column of the report table in the database based on
    which report types where identified upon fetching them from the database"""
    try:
        cursor = pgdb_connection.cursor()
        reports = [(report.get_report_id(), report.get_report_type()) 
                    for report in batch_validation_report 
                    if report.get_validation_result() == report_constants.VALIDATION_RESULTS["SUCCESS"]
                  ]
        national_reports_ids = [report.get_report_id() 
                                    for report in batch_validation_report 
                                    if report.get_validation_result() == report_constants.VALIDATION_RESULTS["SUCCESS"] 
                                       and report.get_report_type() == report_constants.REPORT_TYPES["NATIONAL"]
                               ]
        if len(national_reports_ids) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET report_type = '{report_constants.REPORT_TYPES["NATIONAL"]}'
                            WHERE report_id IN {helpers.get_sql_list_str(national_reports_ids)}""")

        state_reports_ids = [report.get_report_id() 
                                for report in batch_validation_report 
                                if report.get_validation_result() == report_constants.VALIDATION_RESULTS["SUCCESS"] 
                                   and report.get_report_type() == report_constants.REPORT_TYPES["STATE"]
                            ]
        if len(state_reports_ids) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET report_type = '{report_constants.REPORT_TYPES["STATE"]}'
                            WHERE report_id IN {helpers.get_sql_list_str(state_reports_ids)}""")
        
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_upload_and_update_dates(batch_validation_report: BatchValidationReport, user_id: int) -> None:
    """update the report_type column of the report table in the database based on
    which report types where identified upon fetching them from the database"""
    try:
        # update the updated dates (all reports requested)
        cursor = pgdb_connection.cursor()
        report_ids = [report.get_report_id()
            for report in batch_validation_report
        ]
        
        if len(report_ids) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET last_updated_date = CURRENT_TIMESTAMP, last_updated_by = {user_id}
                            WHERE report_id IN {helpers.get_sql_list_str(report_ids)}""")
        
        # update the uploaded dates (only the sucessfully validated reports)
        succesfully_validated_report_ids = [
            report.get_report_id()
            for report in batch_validation_report 
            if report.get_validation_result() == report_constants.VALIDATION_RESULTS["SUCCESS"]
        ]
        
        if len(succesfully_validated_report_ids) > 0:
            cursor.execute(f"""UPDATE {db_constants.DB_TABLES["REPORT"]}
                            SET last_uploaded_date = CURRENT_TIMESTAMP, last_uploaded_by = {user_id}
                            WHERE report_id IN {helpers.get_sql_list_str(succesfully_validated_report_ids)}""")

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None