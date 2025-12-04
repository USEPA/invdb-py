from chalicelib.src.source_files.jobs.load.queries import *
import chalicelib.src.source_files.jobs.qc_extraction.methods as qc_extraction_methods
import chalicelib.src.source_files.jobs.qc_extraction.queries as qc_extraction_queries
import chalicelib.src.source_files.jobs.crt_extraction.methods as crt_extraction_methods
import chalicelib.src.source_files.jobs.crt_extraction.queries as crt_extraction_queries
import chalicelib.src.source_files.constants as source_file_constants
import chalicelib.src.general.globals as invdb_globals
from chalicelib.src.jobs.models.Job import Job as Job_Class
import chalicelib.src.jobs.constants as job_constants
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import jsonify
import traceback
import hashlib
import os

<<<<<<< HEAD
dim_table_foreign_keys = fetch_dim_table_foreign_keys()
data_type_foreign_keys = {
    pair[0]: pair[1] for pair in dim_table_foreign_keys.get("data_type")
}
sector_foreign_keys = {
    pair[0]: pair[1] for pair in dim_table_foreign_keys.get("sector")
}
subsector_foreign_keys = {
    pair[0]: pair[1] for pair in dim_table_foreign_keys.get("subsector")
}
category_foreign_keys = {
    pair[0]: pair[1] for pair in dim_table_foreign_keys.get("category")
}
fuel_foreign_keys = {pair[0]: pair[1] for pair in dim_table_foreign_keys.get("fuel")}
ghg_code_foreign_keys = {
    quintuple[0]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
}
ghg_longname_foreign_keys = {
    quintuple[1]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
}
ghg_shortname_foreign_keys = {
    quintuple[2]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
}
cas_no_foreign_keys = {
    quintuple[3]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
}
=======
# initialize global variables
dim_table_foreign_keys = None
data_type_foreign_keys = None
sector_foreign_keys = None
subsector_foreign_keys = None
category_foreign_keys = None
fuel_foreign_keys = None
ghg_code_foreign_keys = None
ghg_longname_foreign_keys = None
ghg_shortname_foreign_keys = None
cas_no_foreign_keys = None


def refresh_foreign_keys():
    global dim_table_foreign_keys
    global data_type_foreign_keys
    global sector_foreign_keys
    global subsector_foreign_keys
    global category_foreign_keys
    global fuel_foreign_keys
    global ghg_code_foreign_keys
    global ghg_longname_foreign_keys
    global ghg_shortname_foreign_keys
    global cas_no_foreign_keys
    dim_table_foreign_keys = fetch_dim_table_foreign_keys()
    data_type_foreign_keys = {
        pair[0]: pair[1] for pair in dim_table_foreign_keys.get("data_type")
    }
    sector_foreign_keys = {
        pair[0]: pair[1] for pair in dim_table_foreign_keys.get("sector")
    }
    subsector_foreign_keys = {
        pair[0]: pair[1] for pair in dim_table_foreign_keys.get("subsector")
    }
    category_foreign_keys = {
        pair[0]: pair[1] for pair in dim_table_foreign_keys.get("category")
    }
    fuel_foreign_keys = {
        pair[0]: pair[1] for pair in dim_table_foreign_keys.get("fuel")
    }
    ghg_code_foreign_keys = {
        quintuple[0]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
    }
    ghg_longname_foreign_keys = {
        quintuple[1]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
    }
    ghg_shortname_foreign_keys = {
        quintuple[2]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
    }
    cas_no_foreign_keys = {
        quintuple[3]: quintuple[4] for quintuple in dim_table_foreign_keys.get("ghg")
    }
>>>>>>> gitlab/develop


def remove_invalid_lines_from_single_source_file(
    source_file, numbers_for_lines_with_errors
):
    """
    input:
        workbook: triple -> ([0]: source_file attachment_id, [1]: source_file_content as openpyxl.workbook, [2]: source_file ID)
        lines_with_errors: a list of ints specifying which rows contain errors and need to be removed.
    output:
        None. (only alters the workbook[1] input)
    """
    # errors rows must be removed in descending order for correctness.
    numbers_for_lines_with_errors.sort(reverse=True)
    for line_number in numbers_for_lines_with_errors:
        source_file.delete_rows(
            line_number, 1
        )  # POTENTIAL OPTIMIZATION: handle consecutive errors rows with a single call to sheet.delete_rows(first row in cluster, cluster size).


def remove_invalid_lines_from_source_files(source_files, source_file_errors):
    """
    input:
        workbooks: a list SourceFiles objects
        source_file_errors: a list of tuples ([0]: source_file attachment_id, [1]: row number of error)
    output:
        None. (only alters the workbooks[1] input)
    """

    # assign each thread its own source file to edit
    with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
        futures = []
        for source_file in source_files:
            # select only errors for the current source file
            errors_for_this_source_file = [
                error[1]
                for error in source_file_errors
                if error[0] == source_file.get_attachment_id()
            ]
            # skip the source file if it doesn't have any errors
            if len(errors_for_this_source_file) == 0:
                continue

            # define the line removal task for a single source file as a job for the multithreading executor
            future = executor.submit(
                remove_invalid_lines_from_single_source_file,
                source_file,
                errors_for_this_source_file,
            )
            futures.append(future)

        executor.shutdown(wait=True)
        return


def replace_single_source_file_values_with_foreign_keys(source_file: SourceFile):
<<<<<<< HEAD
    global dim_table_foreign_keys
    global data_type_foreign_keys
    global sector_foreign_keys
    global subsector_foreign_keys
    global category_foreign_keys
    global fuel_foreign_keys
    global ghg_code_foreign_keys
    global ghg_longname_foreign_keys
    global ghg_shortname_foreign_keys
    global cas_no_foreign_keys

    template = source_file.get_template()
    for row_data in source_file:
        data_type_col_pos = source_file_constants.POSITIONS[template]['DATA_TYPE_COL_POS']
        # replace the data type value
        if (
            row_data[data_type_col_pos] not in (None, "NULL")
=======
    template = source_file.get_template()
    for row_data in source_file:
        data_type_col_pos = source_file_constants.POSITIONS[template][
            "DATA_TYPE_COL_POS"
        ]
        # replace the data type value
        if row_data[data_type_col_pos] not in (
            None,
            "NULL",
>>>>>>> gitlab/develop
        ):  # data type is an optional value
            if row_data[data_type_col_pos] in data_type_foreign_keys:
                row_data[data_type_col_pos] = data_type_foreign_keys[
                    row_data[data_type_col_pos]
                ]
            else:
                row_data[data_type_col_pos] = 999999

        # replace the sector value
<<<<<<< HEAD
        sector_col_pos = source_file_constants.POSITIONS[template]['SECTOR_COL_POS']
        try:
            row_data[sector_col_pos] = sector_foreign_keys[
                row_data[sector_col_pos]
            ]
=======
        sector_col_pos = source_file_constants.POSITIONS[template]["SECTOR_COL_POS"]
        try:
            row_data[sector_col_pos] = sector_foreign_keys[row_data[sector_col_pos]]
>>>>>>> gitlab/develop
        except KeyError:
            row_data[sector_col_pos] = 999999

        # replace the subsector value
<<<<<<< HEAD
        subsector_col_pos = source_file_constants.POSITIONS[template]['SUBSECTOR_COL_POS']
=======
        subsector_col_pos = source_file_constants.POSITIONS[template][
            "SUBSECTOR_COL_POS"
        ]
>>>>>>> gitlab/develop
        try:
            row_data[subsector_col_pos] = subsector_foreign_keys[
                row_data[subsector_col_pos]
            ]
        except KeyError:
            row_data[subsector_col_pos] = 999999

        # replace the category value
<<<<<<< HEAD
        category_col_pos = source_file_constants.POSITIONS[template]['CATEGORY_COL_POS']
        if (
            row_data[category_col_pos] not in (None, "NULL")
=======
        category_col_pos = source_file_constants.POSITIONS[template]["CATEGORY_COL_POS"]
        if row_data[category_col_pos] not in (
            None,
            "NULL",
>>>>>>> gitlab/develop
        ):  # category is an optional value
            try:
                row_data[category_col_pos] = category_foreign_keys[
                    row_data[category_col_pos]
                ]
            except KeyError:
                row_data[category_col_pos] = 999999

        # replace the fuel values
<<<<<<< HEAD
        fuel1_col_pos = source_file_constants.POSITIONS[template]['FUEL1_COL_POS']
        fuel2_col_pos = source_file_constants.POSITIONS[template]['FUEL2_COL_POS']
        if (
            row_data[fuel1_col_pos] not in (None, "NULL")
        ):  # fuel1 is an optional value
            try:
                row_data[fuel1_col_pos] = fuel_foreign_keys[
                    row_data[fuel1_col_pos]
                ]
            except KeyError:
                row_data[fuel1_col_pos] = 999999
        if (
            row_data[fuel2_col_pos] not in (None, "NULL")
        ):  # fuel2 is an optional value
            try:
                row_data[fuel2_col_pos] = fuel_foreign_keys[
                    row_data[fuel2_col_pos]
                ]
=======
        fuel1_col_pos = source_file_constants.POSITIONS[template]["FUEL1_COL_POS"]
        fuel2_col_pos = source_file_constants.POSITIONS[template]["FUEL2_COL_POS"]
        if row_data[fuel1_col_pos] not in (None, "NULL"):  # fuel1 is an optional value
            try:
                row_data[fuel1_col_pos] = fuel_foreign_keys[row_data[fuel1_col_pos]]
            except KeyError:
                row_data[fuel1_col_pos] = 999999
        if row_data[fuel2_col_pos] not in (None, "NULL"):  # fuel2 is an optional value
            try:
                row_data[fuel2_col_pos] = fuel_foreign_keys[row_data[fuel2_col_pos]]
>>>>>>> gitlab/develop
            except KeyError:
                row_data[fuel2_col_pos] = 999999

        # replace the GHG value (look into one of 4 different values)
<<<<<<< HEAD
        ghg_col_pos = source_file_constants.POSITIONS[template]['GHG_COL_POS']
        if row_data[ghg_col_pos] in ghg_code_foreign_keys:
            row_data[ghg_col_pos] = ghg_code_foreign_keys[
                row_data[ghg_col_pos]
            ]
        elif row_data[ghg_col_pos] in ghg_longname_foreign_keys:
            row_data[ghg_col_pos] = ghg_longname_foreign_keys[
                row_data[ghg_col_pos]
            ]
        elif row_data[ghg_col_pos] in ghg_shortname_foreign_keys:
            row_data[ghg_col_pos] = ghg_shortname_foreign_keys[
                row_data[ghg_col_pos]
            ]
        elif row_data[ghg_col_pos] in cas_no_foreign_keys:
            row_data[ghg_col_pos] = cas_no_foreign_keys[
                row_data[ghg_col_pos]
            ]
        else:    
=======
        ghg_col_pos = source_file_constants.POSITIONS[template]["GHG_COL_POS"]
        if row_data[ghg_col_pos] in ghg_code_foreign_keys:
            row_data[ghg_col_pos] = ghg_code_foreign_keys[row_data[ghg_col_pos]]
        elif row_data[ghg_col_pos] in ghg_longname_foreign_keys:
            row_data[ghg_col_pos] = ghg_longname_foreign_keys[row_data[ghg_col_pos]]
        elif row_data[ghg_col_pos] in ghg_shortname_foreign_keys:
            row_data[ghg_col_pos] = ghg_shortname_foreign_keys[row_data[ghg_col_pos]]
        elif row_data[ghg_col_pos] in cas_no_foreign_keys:
            row_data[ghg_col_pos] = cas_no_foreign_keys[row_data[ghg_col_pos]]
        else:
>>>>>>> gitlab/develop
            row_data[ghg_col_pos] = 999999


def batch_replace_values_with_foreign_keys(source_files):
    """
    uses multithreading to process foreign key substitution operation for multiple source files in parallel
    input:
        workbooks: a list of triples -> ([0]: source_file attachment_id, [1]: source_file_content as openpyxl.workbook, [2]: source_file ID)
        foreign_keys: dict ->   key: column name,
                                value: dict ->  key: column value
                                                value: foreign key
    output:
        None. (only alters the workbooks[1] input)
    """
<<<<<<< HEAD
    if invdb_globals.allow_multithreading:
    #=================== multi-threaded version ========================
=======
    # refresh dim table foreign keys before load process so we don't see any outdated or stale data
    refresh_foreign_keys()
    if invdb_globals.allow_multithreading:
        # =================== multi-threaded version ========================
>>>>>>> gitlab/develop
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for source_file in source_files:
                # define the line removal task for a single source file as a job for the executor
                future = executor.submit(
                    replace_single_source_file_values_with_foreign_keys,
                    source_file,
                )
                futures.append(future)

            executor.shutdown(wait=True)
            return
<<<<<<< HEAD
    #=================== single-threaded version ========================
    else:
        for source_file in source_files:
            replace_single_source_file_values_with_foreign_keys(source_file)
    #====================================================================
=======
    # =================== single-threaded version ========================
    else:
        for source_file in source_files:
            replace_single_source_file_values_with_foreign_keys(source_file)
    # ====================================================================

>>>>>>> gitlab/develop

def extract_archive_data_from_single_source_file(source_file: SourceFile):
    """
    input:
        workbook:  triple -> ([0]: source_file attachment_id, [1]: source_file_content as openpyxl.workbook, [2]: source_file ID)
    """
    try:
        template = source_file.get_template()
        emission_data_type_ids = fetch_emissions_key_data_type_ids()
        activity_data_type_ids = fetch_activity_key_data_type_ids()
        source_file_id = source_file.get_source_file_id()
<<<<<<< HEAD
        activity_key_data = [] # activity (AF) data
=======
        activity_key_data = []  # activity (AF) data
>>>>>>> gitlab/develop
        emissions_key_data = []  # the blue columns from the source file 2.0 template
        emissions_quantity_data = (
            []
        )  # the gray columns from the source file 2.0 template
        i = 0
        for row_data in source_file:
<<<<<<< HEAD
            num_emission_key_columns = source_file_constants.INFO[template]['NUM_EMISSION_KEY_COLUMNS']
            data_type_col_pos = source_file_constants.POSITIONS[template]['DATA_TYPE_COL_POS']
=======
            num_emission_key_columns = source_file_constants.INFO[template][
                "NUM_EMISSION_KEY_COLUMNS"
            ]
            data_type_col_pos = source_file_constants.POSITIONS[template][
                "DATA_TYPE_COL_POS"
            ]
>>>>>>> gitlab/develop

            is_emission_row = row_data[data_type_col_pos] in emission_data_type_ids
            is_activity_row = row_data[data_type_col_pos] in activity_data_type_ids

            if is_emission_row or is_activity_row:
                # generate the uid for the emission key
                uid_base = row_data[1 : num_emission_key_columns + 1]
<<<<<<< HEAD
                if template == 3: # exclude ghg_category from uid hash if template 3
                    ghg_category_col_pos = source_file_constants.POSITIONS[template]['GHG_CATEGORY_COL_POS']
                    uid_base = row_data[1 : ghg_category_col_pos] + row_data[ghg_category_col_pos + 1 : num_emission_key_columns + 1]
=======
                if template == 3:  # exclude ghg_category from uid hash if template 3
                    ghg_category_col_pos = source_file_constants.POSITIONS[template][
                        "GHG_CATEGORY_COL_POS"
                    ]
                    uid_base = (
                        row_data[1:ghg_category_col_pos]
                        + row_data[
                            ghg_category_col_pos + 1 : num_emission_key_columns + 1
                        ]
                    )
>>>>>>> gitlab/develop
                emissions_key_uid = hashlib.md5(
                    str(tuple(uid_base)).encode(),
                    usedforsecurity=False,
                ).hexdigest()

                # add the emission/activity key data as a tuple with its uid
                (emissions_key_data if is_emission_row else activity_key_data).append(
                    (row_data[data_type_col_pos], emissions_key_uid)
<<<<<<< HEAD
                    + tuple(row_data[1 : num_emission_key_columns]) + (source_file_id,)
=======
                    + tuple(row_data[1:num_emission_key_columns])
                    + (source_file_id,)
>>>>>>> gitlab/develop
                )

                # facts_archive data
                emissions_quantity_data.append(
                    (
                        (
                            emissions_key_uid,
                            row_data[data_type_col_pos],
                            source_file.get_attachment_id(),
                        ),
<<<<<<< HEAD
                        tuple(row_data[num_emission_key_columns :]),
=======
                        tuple(row_data[num_emission_key_columns:]),
>>>>>>> gitlab/develop
                    )
                )
            i += 1

    except Exception as e:
        helpers.tprint(
            f"Failed to load source file with attachment ID: {source_file.get_attachment_id()}. Refer to traceback below:"
        )
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return {
            "activity_key_data": [],
            "emissions_key_data": [],
            "emissions_quantity_data": [],
        }

    return {
        "activity_key_data": activity_key_data,
        "emissions_key_data": emissions_key_data,
        "emissions_quantity_data": emissions_quantity_data,
    }


def extract_archive_data_from_source_files(source_files):
    """
    Extract all lines of data from the input source files where data_type == "Emission" and return a dict holding the key data (blue columns) and quantity data (gray columns)
    input:
        workbooks: a list of triples -> ([0]: source_file attachment_id, [1]: source_file_content as openpyxl.workbook, [2]: source_file ID)
    output:
        dict -> {
                    "emissions_key_data": list -> emissions key data (blue column data for all source files' rows where data_type == "Emission")
                    "emissions_quantity_data": list -> emissions quantity data (gray column data for all source files' rows where data_type == "Emission")
                }
    """
    activity_key_data = []
    emissions_key_data = []  # the blue columns from the source file 2.0 template
    emissions_quantity_data = []  # the gray columns from the source file 2.0 template
    if invdb_globals.allow_multithreading:
<<<<<<< HEAD
    #=================== multi-threaded version ========================
=======
        # =================== multi-threaded version ========================
>>>>>>> gitlab/develop
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for source_file in source_files:
                future = executor.submit(
                    extract_archive_data_from_single_source_file,
                    source_file,
                )
                futures.append(future)

            for future in as_completed(futures):
                activity_key_data += future.result()["activity_key_data"]
                emissions_key_data += future.result()["emissions_key_data"]
                emissions_quantity_data += future.result()["emissions_quantity_data"]

            executor.shutdown(wait=True)
<<<<<<< HEAD
    #=================== single-threaded version ========================
    else: 
=======
    # =================== single-threaded version ========================
    else:
>>>>>>> gitlab/develop
        for source_file in source_files:
            result = extract_archive_data_from_single_source_file(source_file)
            activity_key_data += result["activity_key_data"]
            emissions_key_data += result["emissions_key_data"]
            emissions_quantity_data += result["emissions_quantity_data"]
<<<<<<< HEAD
    #====================================================================
=======
    # ====================================================================
>>>>>>> gitlab/develop

    return {
        "activity_key_data": activity_key_data,
        "emissions_key_data": emissions_key_data,
        "emissions_quantity_data": emissions_quantity_data,
    }


<<<<<<< HEAD
def handle_source_file_archiving_request(reporting_year: int,   layer_id: int,   user_id: int,   debug: bool=None,   ids: [int]=None):
=======
def handle_source_file_archiving_request(
    reporting_year: int,
    layer_id: int,
    user_id: int,
    debug: bool = None,
    ids: [int] = None,
):
>>>>>>> gitlab/develop
    this_job = Job_Class(
        job_constants.SOURCE_FILE_LOAD_NAME,
        job_constants.SOURCE_FILE_LOAD_DESC,
        reporting_year,
        layer_id,
<<<<<<< HEAD
        user_id
=======
        user_id,
>>>>>>> gitlab/develop
    )
    try:
        if debug is not None:
            debug_save = invdb_globals.debug
            invdb_globals.debug = debug

        helpers.tprint("Fetching source files for loading...")
        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "FETCHING_FILES",
        )
        source_files = fetch_unarchived_source_files(reporting_year, layer_id, ids)
        if len(source_files) == 0:
            result_str = """No source files to load. (Either no files were found, or all have already been archived.)
            NOTE: source files must undergo validation before they can be archived."""
            helpers.tprint(result_str)
            this_job.update_status("COMPLETE")
            helpers.tprint(f"Done.")
            return jsonify({"result": result_str}), 200

        helpers.tprint(f"\tSource files found: {source_files}")

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "FETCHING_ERRORS",
        )
        helpers.tprint("Fetching validation errors for source files...")
        errors = fetch_source_file_validation_error_rows(reporting_year, layer_id)

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "OPENING_FILES",
        )
        helpers.tprint("Opening the source files for editing...")
        for source_file in source_files:
            source_file.open()

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "REMOVING_INVALID_DATA",
        )
        helpers.tprint("Removing invalid data...")
        remove_invalid_lines_from_source_files(source_files, errors)

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "MAPPING_DATA",
        )
        helpers.tprint("Mapping data to foreign keys...")
        batch_replace_values_with_foreign_keys(source_files)

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "GATHERING_DATA",
        )
        helpers.tprint("Gathering emission key and facts data...")
        archive_data = extract_archive_data_from_source_files(source_files)

        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "UPDATING_DATABASE",
        )
        helpers.tprint("Updating the database...")
        update_facts_archive_table(
            archive_data["emissions_quantity_data"],
            reporting_year,
            layer_id,
<<<<<<< HEAD
            [source_file.get_source_file_id() for source_file in source_files]
        )
        update_emissions_key_table(archive_data["emissions_key_data"], source_file.get_template())
        update_activity_key_table(archive_data["activity_key_data"], source_file.get_template())
=======
            [source_file.get_source_file_id() for source_file in source_files],
        )
        update_emissions_key_table(
            archive_data["emissions_key_data"], source_file.get_template()
        )
        update_activity_key_table(
            archive_data["activity_key_data"], source_file.get_template()
        )
>>>>>>> gitlab/develop
        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "COMPLETED_LOAD",
        )
        helpers.tprint(f"Source file emissions data archiving complete.")
<<<<<<< HEAD
               
=======

>>>>>>> gitlab/develop
        if debug is not None:
            invdb_globals.debug = debug_save
        source_name_ids = [x.get_source_name_id() for x in source_files]

        # perform QC Extraction on the source files
<<<<<<< HEAD
        qc_results = qc_extraction_methods.extract_qc_data_from_source_files(source_files)
        for result in qc_results:
            qc_extraction_queries.update_qc_facts_archive_table(result["facts"])
            qc_extraction_queries.update_emissionsqc_key_table(result["keys"])
            qc_extraction_queries.update_qc_validation_error_logs(result["errors"], source_name_ids, reporting_year, layer_id, user_id)

        # perform CRT Extraction on the source files
        crt_results = crt_extraction_methods.extract_crt_data_from_source_files(source_files)
=======
        qc_results = qc_extraction_methods.extract_qc_data_from_source_files(
            source_files
        )
        for result in qc_results:
            qc_extraction_queries.update_qc_facts_archive_table(result["facts"])
            qc_extraction_queries.update_emissionsqc_key_table(result["keys"])
            qc_extraction_queries.update_qc_validation_error_logs(
                result["errors"], source_name_ids, reporting_year, layer_id, user_id
            )

        # perform CRT Extraction on the source files
        crt_results = crt_extraction_methods.extract_crt_data_from_source_files(
            source_files
        )
>>>>>>> gitlab/develop
        for result in crt_results:
            crt_extraction_queries.update_facts_archive_table(result["facts"])
            crt_extraction_queries.update_crt_key_table(result["keys"])

        update_source_file_processed_dates(
            [source_file.get_attachment_id() for source_file in source_files], user_id
        )
<<<<<<< HEAD
        
=======

>>>>>>> gitlab/develop
        # refresh rollup tables
        helpers.tprint(f"Refreshing rollup tables")
        this_job.post_event(
            "SOURCE_FILE_LOAD",
            "UPDATING_SUMMARY_TABLES",
        )
        update_refresh_status_rollup_table(reporting_year, layer_id)
        update_emissions_rollup_tables(reporting_year, layer_id)
<<<<<<< HEAD
        
=======

>>>>>>> gitlab/develop
        this_job.update_status("COMPLETE")
        return jsonify({"result": "Load complete."}), 200
    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
<<<<<<< HEAD
=======

>>>>>>> gitlab/develop
        this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
<<<<<<< HEAD
        return jsonify({"traceback": traceback_obj}), 500
=======
        return jsonify({"traceback": traceback_obj}), 500
>>>>>>> gitlab/develop
