from chalicelib.src.source_files.jobs.validation.queries import *
from chalicelib.src.source_files.models.ValidationReport import ValidationReport
from chalicelib.src.source_files.models.DataQualityError import DataQualityError
import chalicelib.src.source_files.constants as source_file_constants
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.database.methods as db_methods
from chalicelib.src.general.models.BatchValidationReport import (
    BatchValidationReport,
)
from chalicelib.src.source_files.models.SourceFile import *
# from chalicelib.src.xxx.jobs.xxx.queries import *
from chalicelib.src.jobs.models.Job import Job as Job_Class
import chalicelib.src.jobs.constants as job_constants
from flask import jsonify
# globals. => invdb_invdb_globals.
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
from concurrent.futures import ThreadPoolExecutor, as_completed
from openpyxl.utils import get_column_letter
import traceback
import os


# define global variables for the file
# fetch the dim table values and max reporting year
def redefine_globals(): 
    global data_type_values
    global sector_values
    global subsector_values
    global category_values
    global fuel_values
    global ghg_values
    
    dim_table_values = db_methods.fetch_dim_table_validation_values()
    data_type_values = dim_table_values.get("data_type")
    sector_values = dim_table_values.get("sector")
    subsector_values = dim_table_values.get("subsector")
    category_values = dim_table_values.get("category")
    fuel_values = dim_table_values.get("fuel")
    ghg_values = dim_table_values.get("ghg")

    # allow Null values for data_type, category, and fuel_values during validation
    data_type_values += [None, "NULL"]
    category_values += [None, "NULL"]
    fuel_values += [None, "NULL"]


def validate_source_file_line(
    source_file_line_data: SourceFileRow, row_num: int, 
    attachment_id: int, template: int, created_by: int = None
) -> [DataQualityError]: 
    """takes a row from a source file and returns a list of found validation errors"""
    global data_type_values
    global sector_values
    global subsector_values
    global category_values
    global fuel_values
    global ghg_values

    errors_on_this_row = []
    data_type_col_pos = source_file_constants.POSITIONS[template]['DATA_TYPE_COL_POS']
    if source_file_line_data[data_type_col_pos] not in data_type_values:
        errors_on_this_row.append(
            DataQualityError(
                "Data Type",
                row_num,
                data_type_col_pos,
                attachment_id,
                qc_constants.DATA_TYPE_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['DATA_TYPE_COL_LABEL'],
                    field_value=data_type_col_pos,
                    dim_values=str(data_type_values).replace("'", ""),
                ),
                created_by=created_by,
            )
        )
    sector_col_pos = source_file_constants.POSITIONS[template]['SECTOR_COL_POS']
    if source_file_line_data[sector_col_pos] not in sector_values:
        errors_on_this_row.append(
            DataQualityError(
                "Sector",
                row_num,
                source_file_line_data[sector_col_pos],
                attachment_id,
                qc_constants.SECTOR_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SECTOR_COL_LABEL'],
                    field_value=source_file_line_data[sector_col_pos],
                ),
                created_by=created_by,
            )
        )
    subsector_col_pos = source_file_constants.POSITIONS[template]['SUBSECTOR_COL_POS']
    if source_file_line_data[subsector_col_pos] not in subsector_values:
        errors_on_this_row.append(
            DataQualityError(
                "Subsector",
                row_num,
                source_file_line_data[subsector_col_pos],
                attachment_id,
                qc_constants.SUBSECTOR_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUBSECTOR_COL_LABEL'],
                    field_value=source_file_line_data[subsector_col_pos],
                ),
                created_by=created_by,
            )
        )
    category_col_pos = source_file_constants.POSITIONS[template]['CATEGORY_COL_POS']
    if source_file_line_data[category_col_pos] not in category_values:
        errors_on_this_row.append(
            DataQualityError(
                "Category",
                row_num,
                source_file_line_data[category_col_pos],
                attachment_id,
                qc_constants.CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['CATEGORY_COL_LABEL'],
                    field_value=source_file_line_data[category_col_pos],
                ),
                created_by=created_by,
            )
        )
    fuel1_col_pos = source_file_constants.POSITIONS[template]['FUEL1_COL_POS']
    if source_file_line_data[fuel1_col_pos] not in fuel_values:
        errors_on_this_row.append(
            DataQualityError(
                "Fuel1",
                row_num,
                source_file_line_data[fuel1_col_pos],
                attachment_id,
                qc_constants.FUEL1_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['FUEL1_COL_LABEL'],
                    field_value=source_file_line_data[fuel1_col_pos],
                ),
                created_by=created_by,
            )
        )
    fuel2_col_pos = source_file_constants.POSITIONS[template]['FUEL2_COL_POS']
    if source_file_line_data[fuel2_col_pos] not in fuel_values:
        errors_on_this_row.append(
            DataQualityError(
                "Fuel2",
                row_num,
                source_file_line_data[fuel2_col_pos],
                attachment_id,
                qc_constants.FUEL2_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['FUEL2_COL_LABEL'],
                    field_value=source_file_line_data[fuel2_col_pos],
                ),
                created_by=created_by,
            )
        )
    # check the list of ghg chemicals for a match
    match_found = False
    ghg_col_pos = source_file_constants.POSITIONS[template]['GHG_COL_POS']
    for chemical in ghg_values:
        if source_file_line_data[ghg_col_pos] in chemical:
            match_found = True
            break
    if not match_found:
        errors_on_this_row.append(
            DataQualityError(
                "GHG",
                row_num,
                source_file_line_data[ghg_col_pos],
                attachment_id,
                qc_constants.GHG_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['GHG_COL_LABEL'],
                    field_value=source_file_line_data[ghg_col_pos],
                ),
                created_by=created_by,
            )
        )

    # Data Type checks for the remaining emission key columns
    # check subcategory1 data type (already tried putting subcategory 1-6 data type checks in a loop)
    sub_category_1_col_pos = source_file_constants.POSITIONS[template]['SUB_CATEGORY_1_COL_POS']
    if source_file_line_data[sub_category_1_col_pos] is not None and (
        not isinstance(source_file_line_data[sub_category_1_col_pos], str)
        or len(source_file_line_data[sub_category_1_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                f"Subcategory1",
                row_num,
                source_file_line_data[sub_category_1_col_pos],
                attachment_id,
                qc_constants.SUB_CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUB_CATEGORY_1_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check subcategory2 data type
    sub_category_2_col_pos = source_file_constants.POSITIONS[template]['SUB_CATEGORY_2_COL_POS']
    if source_file_line_data[sub_category_2_col_pos] is not None and (
        not isinstance(source_file_line_data[sub_category_2_col_pos], str)
        or len(source_file_line_data[sub_category_2_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                f"Subcategory2",
                row_num,
                source_file_line_data[sub_category_2_col_pos],
                attachment_id,
                qc_constants.SUB_CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUB_CATEGORY_2_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check subcategory3 data type
    sub_category_3_col_pos = source_file_constants.POSITIONS[template]['SUB_CATEGORY_3_COL_POS']
    if source_file_line_data[sub_category_3_col_pos] is not None and (
        not isinstance(source_file_line_data[sub_category_3_col_pos], str)
        or len(source_file_line_data[sub_category_3_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                f"Subcategory3",
                row_num,
                source_file_line_data[sub_category_3_col_pos],
                attachment_id,
                qc_constants.SUB_CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUB_CATEGORY_3_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check subcategory4 data type
    sub_category_4_col_pos = source_file_constants.POSITIONS[template]['SUB_CATEGORY_4_COL_POS']
    if source_file_line_data[sub_category_4_col_pos] is not None and (
        not isinstance(source_file_line_data[sub_category_4_col_pos], str)
        or len(source_file_line_data[sub_category_4_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                f"Subcategory4",
                row_num,
                source_file_line_data[sub_category_4_col_pos],
                attachment_id,
                qc_constants.SUB_CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUB_CATEGORY_4_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check subcategory5 data type
    sub_category_5_col_pos = source_file_constants.POSITIONS[template]['SUB_CATEGORY_5_COL_POS']
    if source_file_line_data[sub_category_5_col_pos] is not None and (
        not isinstance(source_file_line_data[sub_category_5_col_pos], str)
        or len(source_file_line_data[sub_category_5_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                f"Subcategory5",
                row_num,
                source_file_line_data[sub_category_5_col_pos],
                attachment_id,
                qc_constants.SUB_CATEGORY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['SUB_CATEGORY_5_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check carbon pool data type
    carbon_pool_col_pos = source_file_constants.POSITIONS[template]['CARBON_POOL_COL_POS']
    if source_file_line_data[carbon_pool_col_pos] is not None and (
        not isinstance(source_file_line_data[carbon_pool_col_pos], str)
        or len(source_file_line_data[carbon_pool_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "Carbon Pool",
                row_num,
                source_file_line_data[carbon_pool_col_pos],
                attachment_id,
                qc_constants.CARBON_POOL_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['CARBON_POOL_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check geo ref data type
    geo_ref_col_pos = source_file_constants.POSITIONS[template]['GEO_REF_COL_POS']
    if source_file_line_data[geo_ref_col_pos] is not None and (
        not isinstance(source_file_line_data[geo_ref_col_pos], str)
        or len(source_file_line_data[geo_ref_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "GeoRef",
                row_num,
                source_file_line_data[geo_ref_col_pos],
                attachment_id,
                qc_constants.GEO_REF_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['GEO_REF_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check exclude data type
    exclude_col_pos = source_file_constants.POSITIONS[template]['EXCLUDE_COL_POS']
    if not source_file_line_data[exclude_col_pos] in [
        "Y",
        "y",
        "N",
        "n",
        None,
        "",
        "NULL"
    ]:
        errors_on_this_row.append(
            DataQualityError(
                "Exclude",
                row_num,
                source_file_line_data[exclude_col_pos],
                attachment_id,
                qc_constants.EXCLUDE_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['EXCLUDE_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check crt code data type
    crt_code_col_pos = source_file_constants.POSITIONS[template]['CRT_CODE_COL_POS']
    if source_file_line_data[crt_code_col_pos] is not None and (
        not isinstance(source_file_line_data[crt_code_col_pos], str)
        or len(source_file_line_data[crt_code_col_pos])
        > qc_constants.CRT_CODE_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "CRT Code",
                row_num,
                source_file_line_data[crt_code_col_pos],
                attachment_id,
                qc_constants.CRT_CODE_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['CRT_CODE_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check id data type
    id_col_pos = source_file_constants.POSITIONS[template]['ID_COL_POS']
    if source_file_line_data[id_col_pos] is not None and (
        not isinstance(source_file_line_data[id_col_pos], str)
        or len(source_file_line_data[id_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "ID",
                row_num,
                source_file_line_data[id_col_pos],
                attachment_id,
                qc_constants.ID_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['ID_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check cbi activity data type
    cbi_activity_col_pos = source_file_constants.POSITIONS[template]['CBI_ACTIVITY_COL_POS']
    if source_file_line_data[cbi_activity_col_pos] is not None and (
        not isinstance(source_file_line_data[cbi_activity_col_pos], str)
        or len(source_file_line_data[cbi_activity_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "CBI Activity / Sensitive",
                row_num,
                source_file_line_data[cbi_activity_col_pos],
                attachment_id,
                qc_constants.CBI_ACTIVITY_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['CBI_ACTIVITY_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check units data type
    units_col_pos = source_file_constants.POSITIONS[template]['UNITS_COL_POS']
    if source_file_line_data[units_col_pos] is not None and (
        not isinstance(source_file_line_data[units_col_pos], str)
        or len(source_file_line_data[units_col_pos])
        > qc_constants.SUB_CATEGORY_MAX_LENGTH
    ):
        errors_on_this_row.append(
            DataQualityError(
                "Units",
                row_num,
                source_file_line_data[units_col_pos],
                attachment_id,
                qc_constants.UNITS_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['UNITS_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # check ghg category length
    if template == 3:
        ghg_cat_pos = source_file_constants.POSITIONS[template]['GHG_CATEGORY_COL_POS']
        ghg_cat_data = source_file_line_data[ghg_cat_pos]
        if ghg_cat_data is not None and len(ghg_cat_data) > qc_constants.GHG_CATEGORY_MAX_LENGTH:
            errors_on_this_row.append(
                DataQualityError(
                    "GHG Category",
                    row_num,
                    source_file_line_data[ghg_cat_pos],
                    attachment_id,
                    qc_constants.GHG_CATEGORY_INVALID_ERROR_MSG.format(
                        row_number=row_num,
                        column_label=source_file_constants.LABELS[template]['GHG_CATEGORY_COL_LABEL'],
                    ),
                    created_by=created_by,
                )
            )


    # check gwp data type
    gwp_col_pos = ghg_cat_pos = source_file_constants.POSITIONS[template]['GWP_COL_POS']
    if (
        source_file_line_data[gwp_col_pos] not in (None, "NULL")
        and not isinstance(source_file_line_data[gwp_col_pos], int)
        and not isinstance(source_file_line_data[gwp_col_pos], float)
    ):
        errors_on_this_row.append(
            DataQualityError(
                "GWP",
                row_num,
                source_file_line_data[gwp_col_pos],
                attachment_id,
                qc_constants.GWP_INVALID_ERROR_MSG.format(
                    row_number=row_num,
                    column_label=source_file_constants.LABELS[template]['GWP_COL_LABEL'],
                ),
                created_by=created_by,
            )
        )

    # iterate through each of the year columns, values must be non-null: numeric or from list constants.YEAR_ALPHA_VALUES
    for col, i in zip(
        range(source_file_constants.INFO[template]['NUM_EMISSION_KEY_COLUMNS'], len(source_file_line_data) + 1),
        range(
            len(
                range(
                    source_file_constants.INFO[template]['NUM_EMISSION_KEY_COLUMNS'], len(source_file_line_data) + 1
                )
            )
        ),
    ):
        if (
            not isinstance(source_file_line_data[col], int)
            and not isinstance(source_file_line_data[col], float)
            and source_file_line_data[col] not in qc_constants.YEAR_ALPHA_VALUES
        ):
            errors_on_this_row.append(
                DataQualityError(
                    f"{1990 + i}",
                    row_num,
                    source_file_line_data[col],
                    attachment_id,
                    qc_constants.YEAR_INVALID_ERROR_MSG.format(
                        row_number=row_num,
                        column_label=get_column_letter(source_file_constants.POSITIONS[template]['Y1990_COL_POS'] + 1 + i),
                        field_value=source_file_line_data[col],
                        dim_values=str(qc_constants.YEAR_ALPHA_VALUES).replace("'", ""),
                    ),
                    created_by=created_by,
                )
            )
    # add any found errors for current row to the total error report
    return errors_on_this_row


def validate_source_file(source_file: SourceFile) -> ValidationReport:
    """takes a single source file object an returns a validation report object containing 
    information on the validation errors found within the source file's contents"""
    if invdb_globals.allow_multithreading:
    # =================== multi-threaded version ========================
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for row_data in source_file:
                futures.append(
                    executor.submit(
                        validate_source_file_line,
                        row_data,
                        row_data.row_number,
                        source_file.get_attachment_id(),
                        source_file.get_template(),
                        source_file.get_created_by(),
                    )
                )

            error_report = ValidationReport(
                source_file.get_source_file_id(), source_file.get_attachment_id()
            )
            for future in as_completed(futures):
                error_report.add_row(future.result())

            executor.shutdown(wait=True)
    # =================== single-threaded version ========================
    else:
        error_report = ValidationReport(
            source_file.get_source_file_id(), source_file.get_attachment_id()
        )
        for row_data in source_file:
            error_report.add_row(
                validate_source_file_line(
                    row_data,
                    row_data.row_number,
                    source_file.get_attachment_id(),
                    source_file.get_template(),
                    source_file.get_created_by(),
                )
            )
    # ====================================================================
        
    source_file._workbook.close()  # for some reason the workbook needs to be closed HERE to release the temp.xlsx file resource for deletion
    return error_report


def execute_batch_source_file_validation(source_files: [SourceFile]) -> BatchValidationReport:
    """takes a list of source files and generates a batch validation report, which has the
    validation reports for all source files passed in the input"""
    # iterate through the source files, generate the validation report for it, and add it to the batch validation report
    redefine_globals()
    batch_validation_report = BatchValidationReport()
    for source_file, i in zip(source_files, range(1, len(source_files) + 1)):
        helpers.tprint(
            f"Validating source file with attachment ID {source_file.get_attachment_id()}. (file {i} of {len(source_files)})"
        )
        # open the source file for reading and generate the validation report
        try:
            with source_file as opened_source_file: #this can raise Errors
                source_file_validation_report = validate_source_file(opened_source_file) # this is can raise Errors
                source_file_validation_report.set_validation_result("SUCCESS") #this can raise ValueError
                batch_validation_report.add_report(source_file_validation_report)
                helpers.tprint(
                    f"Done. ({len(source_file_validation_report.generate_error_list())} {helpers.plurality_agreement('error', 'errors', len(source_file_validation_report.generate_error_list()))} found)"
                )
        except Exception as e:
            traceback_obj = traceback.format_exc()
            helpers.tprint(
                f"Failed to validate source file with attachment ID: {source_file.get_attachment_id()}. Refer to traceback below:"
            )
            failed_source_file_validation_report = ValidationReport(
                source_file.get_source_file_id(), source_file.get_attachment_id()
            )
            failed_source_file_validation_report.set_validation_result("FAILED")
            batch_validation_report.add_report(failed_source_file_validation_report)
            helpers.tprint(traceback_obj)

    return batch_validation_report


def handle_source_file_validation_request(reporting_year: int,   layer_id: int,   user_id: int,   debug: bool=None,   ids: [int]=None):
    this_job = Job_Class(
        job_constants.SOURCE_FILE_VALIDATION_NAME,
        job_constants.SOURCE_FILE_VALIDATION_DESC,
        reporting_year,
        layer_id,
        user_id
    )

    try:
        if debug is not None:
            debug_save = invdb_globals.debug
            invdb_globals.debug = debug

        helpers.tprint("Fetching source files from the database...")
        source_files = fetch_unvalidated_source_files(reporting_year, layer_id, ids)
        if len(source_files) == 0:
            result_str = "No source files to validate. (Either no files were found, or all have already been validated.)"
            helpers.tprint(result_str)
            this_job.update_status("COMPLETE")
            helpers.tprint(f"Done.")
            return jsonify({"result": result_str}), 200

        helpers.tprint(f"\tSource files found: {source_files}\n")
        batch_validation_report = execute_batch_source_file_validation(source_files)
        helpers.tprint(f"The results are:\n{batch_validation_report}")
        source_file_ids = [
            source_file.get_source_file_id() for source_file in source_files
        ]

        helpers.tprint("Updating the database...")
        batch_update_validation_logs(batch_validation_report)
        batch_update_source_file_validation_flags(batch_validation_report)
        batch_update_attachment_has_errors_flags(batch_validation_report)
        delete_failed_attachments_from_database(batch_validation_report)

        this_job.update_status("COMPLETE")
        helpers.tprint(f"Done.")

        if debug is not None:
            invdb_globals.debug = debug_save

        result_str = ""
        if len(batch_validation_report.get_validation_reports_that_failed()) == 0:
            result_str += "All source files validated successfully. "
        else: 
            result_str += "Some or all source files failed to validate. "
        if batch_validation_report.has_reports_with_errors():
            result_str += "Errors found."
        else:
            result_str += "No errors."
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
