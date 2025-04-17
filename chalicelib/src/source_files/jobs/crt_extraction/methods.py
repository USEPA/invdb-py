from chalicelib.src.source_files.models.SourceFile import SourceFile
import chalicelib.src.source_files.jobs.crt_extraction.constants as crt 
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.general.helpers as helpers
from concurrent.futures import ThreadPoolExecutor, as_completed
import traceback
import hashlib
import re
import os


def extract_crt_data_from_single_source_file(source_file: SourceFile):
    try:
        if source_file.get_extension() == '.csv' or source_file.get_extension() == '.json':
            helpers.tprint(f"Skipping CRT extraction for {source_file.get_extension()} file...")
            return {
                "keys": [],
                "facts": [],
            }
        helpers.tprint(f"Extracting CRT data from source file with attachment ID {source_file.get_attachment_id()} and reporting year {source_file.get_reporting_year()}")
        keys = []
        facts = []

        with source_file as file: 
            tab_found = False
            
            # locate the CRT tab with expect CRT Headers row
            for tab_name in crt.CRT_TAB_NAMES:
                # check if tab name exists
                if file.get_sheet_name_if_exists(tab_name):
                    file.set_active_sheet(tab_name)
                    
                    # now find the first CRT heading
                    row_num = 1
                    while row_num <= file._sheet.max_row:  # Iterate from the first row to the last
                        if file._sheet[f'A{row_num}'].value == 'UID':
                            tab_found = True
                            break
                        row_num += 1

                    if tab_found:
                        break

            if not tab_found:
                helpers.tprint(f"Input source file (attachment ID: {file.get_attachment_id()}) is missing its CRT tab (such as 'CRT Input'). Skipping...")
                return {
                    "keys": [],
                    "facts": [],
                }

            time_series_length = source_file.get_max_time_series() - qc_constants.EARLIEST_REPORTING_YEAR + 1
            col_positions = crt.COLUMN_POSITIONS_BY_VERSION[file.get_template()]
            col_positions["NE_IE_COMMENT"] = col_positions["Y1990"] + time_series_length
            col_positions["IE_REPORTED_WHERE"] = col_positions["Y1990"] + time_series_length + 1

            common_crt_key_pattern = re.compile(r'^[a-fA-F0-9-]{36}$')
            country_specific_crt_key_pattern = re.compile(r'^[a-fA-F0-9]{24}$')
            capitalized_word_start_pattern = re.compile(r'[A-Z][a-z]')
            
            current_headers = [None, None, None, None] # represents header_1, header_2, header_3, and header_4
            crt_key_data = []
            crt_facts_data = []
            
            if row_num >= file._sheet.max_row:
                helpers.tprint(f"Input source file (attachment ID: {file.get_attachment_id()}) header row cannot be located. Skipping...")
                return {
                    "keys": [],
                    "facts": [],
                }

            row_num = 1
            top_header_level = None
            headers_using_labels = True
            bad_header_printed_once = False
            for step in [1, 2]: # gather step 1 info, and then step 2 info (process is the same)
                # navigate to the first row of the step 
                while row_num <= file._sheet.max_row:  # Iterate from the first row to the last
                    if file._sheet[f'A{row_num}'].value == 'UID':
                        break
                    row_num += 1
                
                while True: 

                    row_num += 1
                    # skip rows with missing UID
                    if file[row_num][col_positions["UID"]] is None: 
                        continue
                    # skip strikethrough content
                    if file.get_cell(row_num, col_positions["UID"]).font.strike: 
                        continue
                    # stop this step once the next step is encountered
                    if file[row_num][col_positions["UID"]].strip().lower()[:4] == "step":
                        break

                    # differentiate headers and data by checking for the UID pattern in the first column
                    is_common_data_row = common_crt_key_pattern.match(file[row_num][col_positions["UID"]]) 
                    is_country_specific_data_row = country_specific_crt_key_pattern.match(file[row_num][col_positions["UID"]]) 
                    
                    # if the pattern isn't found, then the current row is a header update
                    if not is_common_data_row and not is_country_specific_data_row: 
                        # determine the level of the header (how many parts are in its label (e.g. 3.A.1. has three parts))
                        if headers_using_labels:
                            header = file[row_num][col_positions["UID"]].strip()
                            last_dot_index = header.rfind(".")
                            if last_dot_index == -1: # no discernable header label found
                                headers_using_labels = False
                            header_title_start = header.find(" ", last_dot_index) 
                            if header_title_start != -1:
                                header_title_start_index = header_title_start + + last_dot_index
                                header_label = header[:header_title_start_index]
                                header_label_parts = header_label.strip().split(".")
                                if header_label_parts[-1] in ["", " "]: # strip any trailing None parts 
                                    header_label_parts = header_label_parts[:-1]
                                if top_header_level is None: 
                                    top_header_level = len(header_label_parts)
                                    current_relative_header_level = 0
                                else: 
                                    # current_relative_header_level (vv) is used to select the INDEX in the current_headers list 
                                    # which correspond to the values in the headers columns header_1, header_2, header_3, and header_4
                                    current_relative_header_level = len(header_label_parts) - top_header_level 
                                if current_relative_header_level < 0:
                                    if not bad_header_printed_once: 
                                        print(f"Attachment ID: {file.get_attachment_id()}, Row {row_num}: There appears to be an inconsistency in header labels, defaulting to header level 1")
                                        bad_header_printed_once = True
                                    current_relative_header_level = 0
                                if current_relative_header_level > 3: 
                                    if not bad_header_printed_once: 
                                        print(f"Attachment ID: {file.get_attachment_id()}, Row {row_num}: There appears to be more than 4 header levels in this file's CRT input. Defaulting to level 4")
                                        bad_header_printed_once = True
                                    current_relative_header_level = 3
                            
                            else: # no discernable header label found
                                headers_using_labels = False
                                if not bad_header_printed_once: 
                                    print(f'Attachment ID: {file.get_attachment_id()}, Row {row_num}: No header label found. But indent level is {file.get_cell(row_num, col_positions["UID"]).alignment.indent}')
                                    bad_header_printed_once = True

                        # update the current_headers list based on the current_relative_header_level and make sure invariants on current_headers are satisfied                        
                        header = file[row_num][col_positions["UID"]].strip()
                        current_headers[current_relative_header_level] = header
                        if current_relative_header_level < len(current_headers) - 1: # clear all lower level headers when a new header is set
                            current_headers[current_relative_header_level + 1:] = [None] * (len(current_headers) - (current_relative_header_level + 1)) 
                        unqiue_non_none_headers = [header for header in helpers.remove_duplicates_from_list(current_headers) if header is not None]
                        current_headers =  unqiue_non_none_headers + ([None] * (len(current_headers) - len(unqiue_non_none_headers))) # makes sure there are no skipped or duplicate header levels
                        if not headers_using_labels: 
                            current_headers = ["Cannot be determined."] * 4

                    else: # process a data row
                        # hashes are based on (UNFCCC UID, CRT input, column C, column D, column E, ne_ie_comment, ie_reported_where)
                        crt_uid = hashlib.md5(
                            str(
                                [file[row_num][col_positions["UID"]], *[file[row_num][col_positions["CRT_INPUT"]:col_positions["Y1990"]]], file[row_num][col_positions["NE_IE_COMMENT"]], file[row_num][col_positions["IE_REPORTED_WHERE"]]]
                            ).encode(),
                            usedforsecurity=False,
                        ).hexdigest()
                        crt_key_data.append((crt_uid, file[row_num][col_positions["UID"]].upper(), ("common" if is_common_data_row else "country-specific"), step,)
                                            + tuple(current_headers)
                                            + tuple(file[row_num][col_positions["CRT_INPUT"]: col_positions["Y1990"]])
                                            + (file[row_num][col_positions["NE_IE_COMMENT"]], file[row_num][col_positions["IE_REPORTED_WHERE"]])
                                            + (file.get_source_file_id(),))
                                            
                        crt_facts_data.append((crt.CRT_DATA_TYPE_ID, crt_uid, 
                                            file.get_layer_id(),
                                            file.get_reporting_year(),
                                            file.get_source_file_id(),
                                            file.get_attachment_id()) + 
                                            tuple(file[row_num][col_positions["Y1990"]: col_positions["NE_IE_COMMENT"]]))
                    

    except Exception as e:
        helpers.tprint(
            f"Failed to extract CRT data from source file with attachment ID: {source_file.get_attachment_id()} and reporting year {source_file.get_reporting_year()}. Refer to traceback below:"
        )
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return {
            "keys": [],
            "facts": [],
        }

    return {
        "keys": crt_key_data,
        "facts": crt_facts_data,
    }


def extract_crt_data_from_source_files(source_files: list[SourceFile]):
    results = []
    if invdb_globals.allow_multithreading:
        helpers.tprint("Beginning extraction with multi-threading")
    #=================== multi-threaded version ========================
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for source_file in source_files:
                future = executor.submit(
                    extract_crt_data_from_single_source_file,
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
            result = extract_crt_data_from_single_source_file(source_file) # dict of keys and facts (2 more dictionaries)
            results.append(result)
    #====================================================================

    return results

# currently not in use
# def handle_crt_extraction_request(source_name_ids: [int], reporting_year: int, layer_id: int, user_id: int, debug: bool):
#     try:
#         if debug is not None:
#             debug_save = invdb_globals.debug
#             invdb_globals.debug = debug

#         # fetch the relevant data product information from the database based on the input arguments
#         helpers.tprint("Fetching source files for crt extraction...")
        
#         if source_name_ids is None or len(source_name_ids) == 0:
#             return {"result": f"No source_name_ids passed in"}
        
#         # remove any duplicate source_name_ids
#         source_name_ids = list(set(source_name_ids))
        
#         source_files = fetch_source_files_by_id(source_name_ids, reporting_year, layer_id) 
#         if len(source_files) == 0:
#             return {"result": f"No Source files with ids {source_name_ids} were returned.  Note: invdb_globals.debug is {invdb_globals.debug}"}

#         # extract a set of results per source file
#         crt_results = extract_crt_data_from_source_files(source_files)
#         """
#         crt_results is a list of these:
#         {
#             keys: [{
#                 hash, parameters, load_target_id
#             }],
#             facts: [{
#                 data_type_id, hash, layer_id, years, time_series, attachment_id
#             }]
#         }
#         """

#         helpers.tprint("Updating the database...")
#         for result in crt_results:
#             update_crt_facts_archive_table(result["facts"])
#             update_emissionscrt_key_table(result["keys"])
#             update_crt_validation_error_logs(result["errors"], source_name_ids, reporting_year, layer_id, user_id)

#         helpers.tprint("Done.")

#         if debug is not None:
#             invdb_globals.debug = debug_save

#         return jsonify({"result": "CRT extraction completed."}), 200

#     except Exception:
#         from chalicelib.src.database.methods import get_pgdb_connection
#         import traceback
#         pgdb_connection = get_pgdb_connection()
#         pgdb_connection.rollback()
#         traceback_obj = traceback.format_exc()
#         helpers.tprint(traceback_obj)
#         return jsonify({"traceback": traceback_obj}), 500