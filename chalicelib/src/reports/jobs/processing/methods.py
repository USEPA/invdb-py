import chalicelib.src.reports.models.Report as Report
from chalicelib.src.reports.models.ReportFactory import *
from chalicelib.src.reports.jobs.processing.queries import *
import chalicelib.src.reports.constants as report_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.jobs.constants as job_constants
from chalicelib.src.jobs.models.Job import Job
from concurrent.futures import ThreadPoolExecutor
from flask import jsonify
import traceback
import os


def create_reports_by_type(report_query_results: list, reporting_year: int) -> [Report]:
    """takes the report information queried from the database and uses the Report Fatory to 
    create the reports based on the reports' contents."""
    max_time_series = db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    reports = []
    report_factory = ReportFactory()
    for report in report_query_results:
        reports.append(
            report_factory.get_report_from_factory(
                report[0],
                report[2],
                max_time_series,
                created_by=report[1],
                read_only=False,
                report_name=report[3]
            )
        )
    return reports


def batch_process_reports(reports: [Report], error_rows: {int: [int]}, query_formula_info, reporting_year: int, layer_id: int, this_job: Job) -> None:
    """takes a list of reports and the errors rows {report_id: [error_row_numbers]}.
    This method will add a bool attribute to each report object denoting whether
    it could process to completion"""
    # assign each thread its own report to edit
    this_job.post_event(
        "REPORT_PROCESSING",
        "OPENING_REPORTS",
    )
    # ===================== multi-threaded mode ==============================
    if False: #invdb_globals.allow_multithreading:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for report in reports:
                try:
                    with report as opened_report:
                        helpers.tprint(f"Processing report with report ID: {opened_report.get_report_id()}...")
                        # define the thread task as processing a single report
                        future = executor.submit(
                            opened_report.process_contents,
                            error_rows[report.get_report_id()],
                            query_formula_info,
                            reporting_year,
                            layer_id,
                            this_job
                        )
                        futures.append(future)
                        report.process_result = "SUCCESS"
                        this_job.post_event(
                            "REPORT_PROCESSING",
                            "REPORT_PROCESSING_SUCCESSFUL",
                            report.get_report_id()
                        )
                except Exception as e:
                    traceback_obj = traceback.format_exc()
                    helpers.tprint(
                        f"Failed to process report with report ID: {report.get_report_id()}. Refer to traceback below:"
                    )
                    report.process_result = "FAILED"
                    this_job.post_event(
                        "REPORT_PROCESSING",
                        "REPORT_PROCESSING_FAILED",
                        report.get_report_id()
                    )
                    helpers.tprint(traceback_obj)
            
            executor.shutdown(wait=True)
    # ========================= single threaded mode ======================
    else:
        for report in reports:
            try:
                with report as opened_report:
                    # define the thread task as processing a single report
                    helpers.tprint(f"Processing report with report ID: {opened_report.get_report_id()}...")
                    opened_report.process_contents(
                        error_rows[report.get_report_id()],
                        query_formula_info,
                        reporting_year,
                        layer_id,
                        this_job
                    )
                    report.process_result = "SUCCESS"
                    this_job.post_event(
                        "REPORT_PROCESSING",
                        "REPORT_PROCESSING_SUCCESSFUL",
                        report.get_report_id()
                    )
            except Exception as e:
                traceback_obj = traceback.format_exc()
                helpers.tprint(
                    f"Failed to process report with report ID: {report.get_report_id()}. Refer to traceback below:"
                )
                report.process_result = "FAILED"
                this_job.post_event(
                    "REPORT_PROCESSING",
                    "REPORT_PROCESSING_FAILED",
                    report.get_report_id()
                )
                helpers.tprint(traceback_obj)
    # =====================================================================


def handle_report_processing_request(reporting_year: int,   layer_id: int,   user_id: int,   debug: bool=None,   ids: [int]=None):
    this_job = Job(
        job_constants.REPORT_PROCESSING_NAME,
        job_constants.REPORT_PROCESSING_DESC,
        reporting_year,
        layer_id,
        user_id
    )

    try:
        if debug is not None:
            debug_save = invdb_globals.debug
            invdb_globals.debug = debug

        helpers.tprint("Fetching reports from the database...")
        this_job.post_event(
            "REPORT_PROCESSING",
            "FETCHING_FILES",
        )
        report_query_results = fetch_validated_reports(reporting_year, layer_id, ids)
        if len(report_query_results) == 0:
            result_str = "No reports to process. (Either no files were found, or all have already been processed.)"
            helpers.tprint(result_str)
            this_job.update_status("COMPLETE")
            helpers.tprint(f"Done.")
            return jsonify({"result": result_str}), 200
        reports = create_reports_by_type(report_query_results, reporting_year)
        helpers.tprint(f"\tReports found: {reports}")

        helpers.tprint("Fetching validation error rows from the database...")
        this_job.post_event(
            "REPORT_PROCESSING",
            "FETCHING_ERRORS",
        )
        error_rows = fetch_report_validation_error_rows(reports)

        query_formula_info = db_methods.fetch_query_formula_name_mappings()
            
        helpers.tprint("Processing reports...")
        batch_process_reports(reports, error_rows, query_formula_info, reporting_year, layer_id, this_job)

        helpers.tprint("Updating reports in the database...")
        batch_update_report_content_in_database(reports)
        batch_update_report_processing_dates_in_database(reports, user_id)

        this_job.update_status("COMPLETE")
        helpers.tprint(f"Done.")

        if debug is not None:
            invdb_globals.debug = debug_save

        # generate the JSON response
        result_str = "REPORTS THAT PROCESSED SUCCESSFULLY:\n"
        reports_thats_processed_successfully = [(report.get_report_id(), report.get_report_name()) for report in reports if report.process_result == "SUCCESS"]
        for id, name in reports_thats_processed_successfully:
            result_str += f"    Report ID: {id}, Report Name: {name}\n"
        if len(reports_thats_processed_successfully) == 0:
            result_str += "    None\n"
        result_str += "\nREPORTS THAT FAILED TO PROCESS:\n"
        reports_that_failed_to_process = [(report.get_report_id(), report.get_report_name()) for report in reports if report.process_result == "FAILED"]
        for id, name in reports_that_failed_to_process:
            result_str += f"    Report ID: {id}, Report Name: {name}\n"
        if len(reports_that_failed_to_process) == 0:
                    result_str += "    None\n"
        
        this_job.post_event(
            "REPORT_PROCESSING",
            "COMPLETED_PROCESSING",
        )
        helpers.tprint(f"The results are:\n{result_str}")
        
        return jsonify({"result": result_str}), 200

    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500