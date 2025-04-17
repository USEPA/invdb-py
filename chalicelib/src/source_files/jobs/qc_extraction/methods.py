from chalicelib.src.source_files.jobs.qc_extraction.queries import *
import chalicelib.src.database.constants as source_file_constants
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.general.helpers as helpers
from chalicelib.src.jobs.models.Job import Job as Job_Class
import chalicelib.src.jobs.constants as job_constants
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import jsonify
import traceback
import hashlib
import re
import os

# turns a cell reference (A3, B28, AX2, etc) into column,row
def split_excel_reference(reference):
    match = re.match(r"([A-Z]+)([0-9]+)", reference)
    if not match:
        raise ValueError("Invalid Excel cell reference")
    
    column, row = match.groups()
    return column, int(row)

def col_to_ascii(letters):
    result = 0
    for char in letters: # to handle multi-letter columns like AA, BX, etc
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result - 1


def extract_qc_data_from_single_source_file(source_file: SourceFile):
    try:
        if source_file.get_extension() == '.csv' or source_file.get_extension() == '.json':
            helpers.tprint(f"Skipping QC extraction for {source_file.get_extension()} file...")
            return {
                "keys": [],
                "facts": [],
                "errors": []
            }
        helpers.tprint(f"Extracting QC data from source file with attachment ID {source_file.get_attachment_id()} and reporting year {source_file.get_reporting_year()}")
        current_target_sheet = None
        load_targets = fetch_qc_load_targets_by_id(source_file.get_source_name_id(), source_file.get_reporting_year())
        keys = []
        facts = []
        errors = []

        if len(load_targets) == 0:
            helpers.tprint(f"[{source_file.get_attachment_id()}, {source_file.get_reporting_year()}] No QC load targets")
        
        if not source_file._is_open:
            source_file.open()

        for load_target in load_targets:
            current_target_sheet = load_target.get_target_tab()
            # swap to specified sheet
            if source_file._sheet.title != current_target_sheet:
                try:
                    source_file.set_active_sheet(current_target_sheet)
                except ValueError:
                    errors.append({
                        "attachment_id": source_file.get_attachment_id(),
                        "load_target_id": load_target.get_emissionsqc_load_target_id(),
                        "error_type": 2, # worksheet/tab name error
                        "description": f"Worksheet `{current_target_sheet}` does not exist",
                        "cell_value": "",
                        "cell_location": ""
                    })
                    continue
            
            title_col_row = split_excel_reference(load_target.get_row_title_cell())
            first_year_col_row = split_excel_reference(load_target.get_data_ref_1990())
            ascii_col_title = col_to_ascii(title_col_row[0])
            ascii_col_first_year = col_to_ascii(first_year_col_row[0])

            row = source_file.__getitem__(title_col_row[1])
            cell_value = str(row[ascii_col_title])
            expected_row_title = load_target.get_anticipated_row_title()
            if cell_value != expected_row_title:
                errors.append({
                    "attachment_id": source_file.get_attachment_id(),
                    "load_target_id": load_target.get_emissionsqc_load_target_id(),
                    "error_type": 1, # anticipated_row_title mismatch
                    "description": f"Anticipated row title `{expected_row_title}` but found `{cell_value}`",
                    "cell_value": cell_value,
                    "cell_location": load_target.get_row_title_cell()
                })
            else:
                num_cols = source_file.get_max_time_series() - source_file_constants.EARLIEST_REPORTING_YEAR
                row_year_tuple = tuple(row[ascii_col_first_year : ascii_col_first_year + num_cols + 1]) # only year cells
                str_hash = str(load_target.get_emissionsqc_load_target_id()) + str(load_target.get_emission_parameters())
                uid = hashlib.md5(
                    str_hash.encode(),
                    usedforsecurity=False,
                ).hexdigest()
                facts.append({
                    "data_type_id": 4,
                    "hash": uid,
                    "layer_id": source_file.get_layer_id(),
                    "years": row_year_tuple,
                    "time_series": source_file.get_max_time_series(),
                    "attachment_id": source_file.get_attachment_id(),
                    "source_file_id": source_file.get_source_file_id()
                })
                keys.append({
                    "hash": uid,
                    "parameters": load_target.get_emission_parameters(),
                    "load_target_id": load_target.get_emissionsqc_load_target_id(),
                    "source_file_id": source_file.get_source_file_id()
                })

    except Exception as e:
        helpers.tprint(
            f"Failed to extract qc from source file with attachment id {source_file.get_attachment_id()} and reporting year {source_file.get_reporting_year()}. Refer to traceback below:"
        )
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return {
            "keys": [],
            "facts": [],
            "errors": errors
        }
    finally:
        source_file.close() 

    return {
        "keys": keys,
        "facts": facts,
        "errors": errors
    }
    


def extract_qc_data_from_source_files(source_files: list[SourceFile]):
    results = []
    if invdb_globals.allow_multithreading:
        helpers.tprint("Beginning extraction with multi-threading")
    #=================== multi-threaded version ========================
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for source_file in source_files:
                future = executor.submit(
                    extract_qc_data_from_single_source_file,
                    source_file,
                )
                futures.append(future)

            for future in as_completed(futures):
                results.append(future.result())

            executor.shutdown(wait=True)
    #=================== single-threaded version ========================
    else: 
        helpers.tprint("Beginning extraction with single thread")
        for source_file in source_files:
            result = extract_qc_data_from_single_source_file(source_file) # dict of keys and facts (2 more dictionaries)
            results.append(result)
    #====================================================================

    return results


def handle_qc_extraction_request(source_name_ids: [int], reporting_year: int, layer_id: int, user_id: int, debug: bool):
    this_job = Job_Class(
        job_constants.QC_EXTRACTION_NAME,
        job_constants.QC_EXTRACTION_DESC,
        reporting_year,
        layer_id,
        user_id,
        misc_info = {"Source Name IDs": source_name_ids}
    )

    try:
        if debug is not None:
            debug_save = invdb_globals.debug
            invdb_globals.debug = debug

        # fetch the relevant data product information from the database based on the input arguments
        helpers.tprint("Fetching source files for preparing qc extraction...")
        
        if source_name_ids is None or len(source_name_ids) == 0:
            return {"result": f"No source_name_ids passed in"}
        
        # remove any duplicate source_name_ids
        source_name_ids = list(set(source_name_ids))
        
        this_job.post_event(
            "QC_EXTRACTION",
            "FETCHING_SOURCE_FILES",
        )

        source_files = fetch_source_files_by_id(source_name_ids, reporting_year, layer_id) 
        if len(source_files) == 0:
            return {"result": f"Source files with ids {source_name_ids} not returned.  Note: invdb_globals.debug is {invdb_globals.debug}"}

        this_job.post_event(
            "QC_EXTRACTION",
            "EXTRACTING_DATA",
        )

        # extract a set of results per source file
        qc_results = extract_qc_data_from_source_files(source_files)
        """
        qc_results is a list of these:
        {
            keys: [{
                hash, parameters, load_target_id
            }],
            facts: [{
                data_type_id, hash, layer_id, years, time_series, attachment_id
            }],
            errors: [{
                attachment_id, load_target_id, error_type, description, cell_value, cell_location
            }]
        }
        """

        helpers.tprint("Updating the database...")
        this_job.post_event(
            "QC_EXTRACTION",
            "WRITING_TO_DATABASE",
        )

        for result in qc_results:
            update_qc_facts_archive_table(result["facts"])
            update_emissionsqc_key_table(result["keys"])
            update_qc_validation_error_logs(result["errors"], source_name_ids, reporting_year, layer_id, user_id)

        helpers.tprint("Done.")
        this_job.update_status("COMPLETE")

        if debug is not None:
            invdb_globals.debug = debug_save

        return jsonify({"result": "QC extraction completed."}), 200

    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500