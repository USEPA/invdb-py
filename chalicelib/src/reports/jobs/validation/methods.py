from chalicelib.src.reports.models.NationalReport import *
from chalicelib.src.reports.models.ValidationReport import ValidationReport
from chalicelib.src.reports.models.DataQualityError import DataQualityError
from chalicelib.src.general.models.BatchValidationReport import BatchValidationReport
import chalicelib.src.database.methods as db_methods
import chalicelib.src.reports.jobs.validation.queries as report_validation_queries
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.reports.constants as report_constants
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
from chalicelib.src.jobs.models.Job import Job as Job_Class
import chalicelib.src.jobs.constants as job_constants
from chalicelib.src.database.methods import *
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import jsonify
import traceback


def redefine_globals():
    global dim_validation_values
    global sector_values
    global subsector_values
    global category_values
    global fuel1_values
    global fuel2_values
    global ghg_longname_values
    global ghg_category_name_values
    global query_formula_info
    global national_query_formulas
    global state_query_formulas
    
    dim_validation_values = db_methods.fetch_dim_table_validation_values(True)
    sector_values = dim_validation_values.get("sector")
    subsector_values = dim_validation_values.get("subsector")
    category_values = dim_validation_values.get("category")
    fuel1_values = dim_validation_values.get("fuel") + [None]
    fuel2_values = dim_validation_values.get("fuel") + [None]
    ghg_longname_values = [
        long_name for (long_name, *_) in dim_validation_values.get("ghg")
    ]
    ghg_category_name_values = dim_validation_values.get("ghg_category")
    query_formula_info = report_validation_queries.fetch_query_formulas_info()
    national_query_formulas = {formula: params for formula, params in query_formula_info.items() if formula[:6] == "em_nat"}
    state_query_formulas = {formula: params for formula, params in query_formula_info.items() if formula[:6] == "em_sta"}


def validate_report_line(
    report_line_data: list, row_num: int, report_id: int, created_by: int, report_type: str
):
    global dim_validation_values
    global sector_values
    global subsector_values
    global category_values
    global fuel1_values
    global fuel2_values
    global ghg_longname_values
    global ghg_category_name_values
    global query_formula_info
    global national_query_formulas
    global state_query_formulas

    errors_on_this_row = []

    # assert that the formula cell is not blank
    if not report_line_data[report_constants.FORMULA_COL_POS] or report_line_data[report_constants.FORMULA_COL_POS].isspace():
        errors_on_this_row.append(
            DataQualityError(
                "Formula",
                row_num,
                "",
                report_id,
                report_constants.BLANK_FORMULA_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=report_constants.FORMULA_COL_LABEL,
                ),
                created_by=created_by,
            )
        )
        return errors_on_this_row # no need to make other checks
    
    formula_string = report_line_data[report_constants.FORMULA_COL_POS].lower().strip() #case insensitive for all parts

    # validate the formula prefix
    formula_string_has_error = False
    formula_prefix = formula_string
    try:
        formula_prefix = formula_string[:formula_string.index("(")]
    except ValueError:
        formula_string_has_error = True
    query_formulas = national_query_formulas if report_type == report_constants.REPORT_TYPES["NATIONAL"] else state_query_formulas
    
    if formula_prefix not in query_formulas.keys() or formula_string_has_error: # if the formula prefix is not recognized
        errors_on_this_row.append( # add an invalid prefix error to this line of the validation report
            DataQualityError(
                "Formula Prefix",
                row_num,
                formula_prefix,
                report_id,
                report_constants.FORMULA_PREFIX_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=report_constants.FORMULA_COL_LABEL,
                    field_value=formula_prefix,
                ),
                created_by=created_by,
            )
        )
    else: # if the formula prefix is valid, validate the parameters
        expected_parameters = query_formulas[formula_prefix]
        input_arguments, formula_has_unquoted_args = helpers.parse_report_formula_arguments(formula_string)

        # make sure all parameters are wrapped in single quotes
        if formula_has_unquoted_args:
            errors_on_this_row.append(
                DataQualityError(
                    f"Formula Syntax",
                    row_num,
                    report_line_data[report_constants.FORMULA_COL_POS],
                    report_id,
                    report_constants.UNQUOTED_PARAMETER_ERROR_MSG.format(
                        row_number=row_num,
                        column_label=report_constants.FORMULA_COL_LABEL,
                    ),
                    created_by=created_by,
                )
            )
            return errors_on_this_row # no need to make other parameter checks

        # make sure the expected number of arguments were passed
        if len(input_arguments) != len(expected_parameters):
            errors_on_this_row.append(
                DataQualityError(
                    f"Parameter Count",
                    row_num,
                    report_line_data[report_constants.FORMULA_COL_POS],
                    report_id,
                    report_constants.PARAMETER_COUNT_INVALID_ERROR_MSG.format(
                        row_number=row_num,
                        column_label=report_constants.FORMULA_COL_LABEL,
                        expected_param_count=len(expected_parameters),
                        input_param_count=len(input_arguments),
                    ),
                    created_by=created_by,
                )
            )
        
        # make sure each parameter value is valid according to dim tables
        for parameter, argument, i in zip(expected_parameters, input_arguments, range(len(input_arguments))):
            if parameter != "SUBCATEGORY1":
                if argument not in eval(f"{parameter.lower()}_values"):
                    errors_on_this_row.append(
                        DataQualityError(
                            f"Parameter {i}: {parameter.upper()}",
                            row_num,
                            argument,
                            report_id,
                            eval(f"qc_constants.{parameter.upper()}_INVALID_ERROR_MSG").format(
                                row_number=row_num,
                                column_label=report_constants.FORMULA_COL_LABEL,
                                field_value=argument,
                            ),
                            created_by=created_by,
                        )
                    )
    
    return errors_on_this_row


def validate_report(report: Report) -> ValidationReport:
    """takes the report path and returns a dictionary of errors.
    Key = row # in the report, value = list of errors encountered"""
    # ================= multi threaded version =====================
    if invdb_globals.allow_multithreading:
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for row_data in report:
                futures.append(
                    executor.submit(
                        validate_report_line,
                        row_data,
                        row_data.row_number,
                        report.get_report_id(),
                        report.get_created_by(),
                        report.get_report_type(),
                    )
                )

            error_report = ValidationReport(report.get_report_id(), report.get_report_type())
            for future in as_completed(futures):
                error_report.add_row(future.result())

            executor.shutdown(wait=True)

    # ================ single threaded version =====================
    else:
        error_report = ValidationReport(report.get_report_id(), report.get_report_type())
        i = 2
        for row_data in report: 
            error_report.add_row(
                        validate_report_line(
                        row_data,
                        row_data.row_number,
                        report.get_report_id(),
                        report.get_created_by(),
                        report.get_report_type(),
                )
            )
            i += 1
    # ===========================================================
    
    # add the missing Query_Results tab error if not present in the report file
    if not report.has_query_results_tab():
        error_report.add_row([DataQualityError(
                                "Query Results Tab",
                                None,
                                "",
                                report.get_report_id(),
                                description=report_constants.MISSING_QUERY_RESULTS_TAB_ERROR_MSG,
                                created_date=None,
                                created_by=report.get_created_by(),
                                tab_name=report_constants.REPORT_OUTPUT_DATA_SHEET_NAME
                            )])

    report._workbook.close()  # for some reason the workbook needs to be closed here to unlock the temp.xlsx resource for deletion

    return error_report


def generate_batch_validation_error_report(reports: [Report]) -> BatchValidationReport:
    """creates a dictionary containing all of the validation errors for all the files passed into by reports.
    validation_reports (type *nested* dict):    [key]: report_id
                                                [value]: dictionary:    [key]: row # in the report containing the errors in the value
                                                                        [value]: a list of QC.classes.DataQualityError.DataQualityError objects
    """
    redefine_globals()
    # iterate through the reports, generate the validation report for it, and add it to the batch validation report
    batch_validation_report = BatchValidationReport()
    for report, i in zip(reports, range(1, len(reports) + 1)):
        helpers.tprint(
            f"Validating report with report ID {report.get_report_id()}. (file {i} of {len(reports)})"
        )
        # open the report for reading and generate the validation report
        try:
            with report as opened_report:
                # run the validation process on the current report
                report_validation_report = validate_report(opened_report) 
                # if this line is reached, the process ran to completion without error
                report_validation_report.set_validation_result("SUCCESS")
                batch_validation_report.add_report(report_validation_report)
                helpers.tprint(
                    f"""Done. ({len(report_validation_report.generate_error_list())} 
                    {helpers.plurality_agreement('error', 'errors', len(report_validation_report.generate_error_list()))} found)"""
                )
        except Exception as e:
            # if this line is reached, the validation process failed part-way.
            traceback_obj = traceback.format_exc()
            helpers.tprint(
                f"Failed to validate report with report ID: {report.get_report_id()}. Refer to traceback below:"
            )
            failed_report_validation_report = ValidationReport(report.get_report_id(), report.get_report_type())
            failed_report_validation_report.set_validation_result("FAILED")
            batch_validation_report.add_report(failed_report_validation_report)
            helpers.tprint(traceback_obj)

    return batch_validation_report


def handle_report_validation_request(reporting_year: int,   layer_id: int,   user_id: int,   debug: bool=None,   ids: [int]=None):
    this_job = Job_Class(
        job_constants.REPORT_VALIDATION_NAME,
        job_constants.REPORT_VALIDATION_DESC,
        reporting_year,
        layer_id,
        user_id
    )

    try:
        if debug is not None:
            debug_save = invdb_globals.debug
            invdb_globals.debug = debug

        helpers.tprint("Fetching reports from the database...")
        reports = report_validation_queries.fetch_unvalidated_reports(reporting_year, layer_id, ids) # use the request information to select the newly updated report for validation
        if len(reports) == 0:
            result_str = "No reports to validate. (Either no files were found, or all have already been validated.)"
            helpers.tprint(result_str)
            this_job.update_status("COMPLETE")
            helpers.tprint(f"Done.")
            return jsonify({"result": result_str}), 200

        helpers.tprint(f"\tReports found: {reports}") 
        batch_validation_report = generate_batch_validation_error_report(reports) # executes the validation process for all the reports and gemerates a report
        helpers.tprint(f"The results are:\n{batch_validation_report}")
        report_ids = [
            report.get_report_id() for report in reports
        ]

        helpers.tprint("Updating the database...")
        errors_found = report_validation_queries.batch_update_validation_log_report_table(batch_validation_report)
        report_validation_queries.batch_update_report_has_errors_flags(batch_validation_report)
        report_validation_queries.batch_update_validation_status_in_database(batch_validation_report)
        report_validation_queries.batch_update_report_types_in_database(batch_validation_report)
        report_validation_queries.batch_update_upload_and_update_dates(batch_validation_report, user_id)
        
        this_job.update_status("COMPLETE")
        helpers.tprint(f"Done.")

        if debug is not None:
            invdb_globals.debug = debug_save

        # return whether validation errors were found or not.
        if errors_found:
            return jsonify({"result": "Errors found."}), 200
        else:
            return jsonify({"result": "No errors."}), 200
        
    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500