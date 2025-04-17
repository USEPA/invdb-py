from chalicelib.src.general.models.BatchValidationReport import *
import chalicelib.src.database.constants as db_constants
import chalicelib.src.source_files.constants as qc_constants
import chalicelib.src.database.methods as db_methods
from chalicelib.src.source_files.models.SourceFile import SourceFile
import chalicelib.src.general.globals as globals
import chalicelib.src.general.helpers as helpers
import os

pgdb_connection = db_methods.get_pgdb_connection()

def fetch_unvalidated_source_files(reporting_year: int, layer_id: int, ids: [int] = None) -> [SourceFile]:
    """returns a list of unvalidated source files from the source_file table 
    of the database for the given reporting year and layer ID"""
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
            SELECT
                a.attachment_id,
                s.created_by, 
                a.content,
                s.source_file_id,
                a.attachment_name
            FROM
                {db_constants.DB_TABLES["SOURCE_FILE"]} s
            JOIN
                {db_constants.DB_TABLES["SOURCE_FILE_ATTACHMENT"]} a ON s.source_file_id = a.source_file_id
            JOIN
                max_dates md ON a.source_file_id = md.source_file_id
            WHERE
                {f'''s.layer_id = {layer_id}
                AND s.reporting_year = {reporting_year}
                AND a.last_srcfile_linked_date = md.max_date''' if ids is None else f'a.attachment_id IN {helpers.get_sql_list_str(ids)}'}
                AND NOT s.is_deleted
                {"AND s.validation_status IS NULL" if not globals.debug else ""}
                """
    )
    source_file_query_results = cursor.fetchall()
    max_time_series = db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    source_files = []
    for source_file in source_file_query_results:
        filename, extension = os.path.splitext(source_file[4])

        source_files.append(
            SourceFile(
                source_file[3],
                source_file[0],
                source_file[2],
                max_time_series,
                reporting_year,
                created_by=source_file[1],
                read_only=True,
                extension=extension
            )
        )
    return source_files


def batch_update_source_file_validation_flags(
    batch_validation_report: [BatchValidationReport],
) -> None:
    """update the validation_status flag of the source_file table based on 
    which source files' validation process could be run to completion"""
    successful_validations = [
        validation_report.get_source_file_id()
        for validation_report in batch_validation_report.get_validation_reports_that_succeeded()
    ]
    failed_validations = [
        validation_report.get_source_file_id()
        for validation_report in batch_validation_report.get_validation_reports_that_failed()
    ]

    cursor = pgdb_connection.cursor()
    try:
        # update successful validations
        if len(successful_validations) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['SOURCE_FILE']} SET validation_status = 'success' WHERE source_file_id IN ({str(successful_validations)[1:-1]}) "
            )
        # update failed validations
        if len(failed_validations) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['SOURCE_FILE']} SET validation_status = 'failed' WHERE source_file_id IN ({str(failed_validations)[1:-1]}) "
            )
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_attachment_has_errors_flags(batch_validation_report: BatchValidationReport) -> None:
    """update the has_errors column of the source_file table in the database based on
    which source files within the input batch validation report had errors"""
    attachment_ids_with_errors = [validation_report.get_attachment_id() for validation_report in batch_validation_report.get_validation_reports_with_errors()]
    attachment_ids_without_errors = [validation_report.get_attachment_id() for validation_report in batch_validation_report.get_validation_reports_without_errors()]

    cursor = pgdb_connection.cursor()
    try:
        if len(attachment_ids_with_errors) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['SOURCE_FILE_ATTACHMENT']} SET has_error = true WHERE attachment_id IN ({str(attachment_ids_with_errors)[1:-1]}) "
            )
        if len(attachment_ids_without_errors) > 0:
            cursor.execute(
                f"UPDATE {db_constants.DB_TABLES['SOURCE_FILE_ATTACHMENT']} SET has_error = false WHERE attachment_id IN ({str(attachment_ids_without_errors)[1:-1]}) "
            )
        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def batch_update_validation_logs(batch_validation_report: BatchValidationReport) -> None:
    """add a row into the validation_log_load table of the database for each 
    validation error contained in the input batch validation report"""
    new_validation_logs = batch_validation_report.generate_error_list()

    # delete the old rows validation logs from the validation_log_load table for the same source files
    try:
        cursor = pgdb_connection.cursor()
        cursor.execute(
            f"""DELETE FROM {db_constants.DB_TABLES['VALIDATION_LOG_LOAD']} 
              WHERE attachment_id IN ({str(batch_validation_report.reports.keys())[11:-2]});"""
        )

        # add the new validation error logs if there are any
        if len(new_validation_logs) > 0:
            # create the SQL literal string for all the new rows
            values_string = ", ".join(
                [
                    f"""({error[0]}, '{error[1]}', '{error[2] if not (isinstance(error[2], str) and len(error[2]) >= 250) else f"{error[2][0:247]}..."}', {error[3]}, '{error[4]}', LOCALTIMESTAMP(6), {error[5] if error[5] is not None else 'NULL'})"""
                    for error in new_validation_logs
                ]
            )
            # insert the new rows into the validation log load table
            cursor.execute(
                f"""INSERT INTO {db_constants.DB_TABLES['VALIDATION_LOG_LOAD']} (attachment_id, field_name, field_value, 
                                                                                row_number, description, created_date, 
                                                                                created_user_id) 
                    VALUES {values_string};"""
            )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None


def delete_failed_attachments_from_database(batch_validation_report: BatchValidationReport) -> None:
    """input: list of tuples: tuple[0]: source_file_id, tuple[1]: attachment_id. Each entry represents a source file that
    failed to validate due to read error, etc."""
    source_file_ids_of_failed_validations = [validation_report.get_source_file_id() for validation_report in batch_validation_report.get_validation_reports_that_failed()]
    attachment_ids_of_failed_validations = [validation_report.get_id() for validation_report in batch_validation_report.get_validation_reports_that_failed()]
    # delete the attachment row
    try:
        cursor = pgdb_connection.cursor()
        if len(attachment_ids_of_failed_validations) > 0:
            cursor.execute(
                f"""DELETE FROM {db_constants.DB_TABLES['SOURCE_FILE_ATTACHMENT']} 
                    WHERE attachment_id IN ({helpers.get_sql_list_str(attachment_ids_of_failed_validations)});"""
            )

        # set the last_attachment_linked_date to NULL for all corresponding source files
        if len(source_file_ids_of_failed_validations) > 0:
            cursor.execute(
                f"""UPDATE {db_constants.DB_TABLES['SOURCE_FILE']} 
                    SET last_attachment_linked_date = NULL
                    WHERE source_file_id IN ({helpers.get_sql_list_str(source_file_ids_of_failed_validations)});"""
            )

        pgdb_connection.commit()
    except Exception as error:
        pgdb_connection.rollback()
        raise error from None
