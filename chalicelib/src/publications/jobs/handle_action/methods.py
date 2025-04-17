import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.publications.jobs.handle_action.queries as pub_queries
import chalicelib.src.general.helpers as helpers
from chalicelib.src.jobs.models.Job import Job
import chalicelib.src.jobs.constants as job_constants
from flask import jsonify
import hashlib
import json
import sys

dim_validation_values = db_methods.fetch_dim_table_id_to_name_mappings()
sector_mappings = dim_validation_values.get("sector")
subsector_mappings = dim_validation_values.get("subsector")
category_mappings = dim_validation_values.get("category")
fuel_mappings = dim_validation_values.get("fuel")
ghg_mappings = dim_validation_values.get("ghg")

state_list = db_methods.fetch_dim_state_list()

def generate_data_key(data: tuple, key_field_positions: [int]=[0]) -> str:
    return hashlib.md5(
        str(
            [data[field] for field in key_field_positions]
        ).encode(),
        usedforsecurity=False,
    ).hexdigest()

def generate_data_object_key(data: dict, key_field_names: [str] = ["data_key"], non_key_field_names: [str] =  None) -> str:
    return hashlib.md5(
        str(
            ([data[field] for field in key_field_names]) if non_key_field_names is None else ([data[field] for field in data.keys() if field not in non_key_field_names])
        ).encode(),
        usedforsecurity=False,
    ).hexdigest()


def pad_data_with_zeroes_for_missing_states_per_data_key(data: [dict], time_series: [int], key_fields: [str]=["data_key"], non_key_fields=[], state_attr_name: str="geo_ref") -> None:
    """based on oracle procedure: t_invdb_pub.proc_create_missing_state_tab2 where all states not reported by a key are inserted into the input data object as a row with all zero quantities.
       Requires that the data includes an data_key value for each row.
       input notes:
        data must be a list of dicts with a 'geo_ref' attribute. 
        non_key_fields must be a list of all the field names that are none of the following (1. key_fields, 2. a quantity value, 3. the state value)"""
    input_data_length = len(data)

    if input_data_length == 0: 
        return
    
    # mark which states have been specified by which keys
    states_listed_by_key = {}
    for row in data:
        current_key = tuple([row[field] for field in key_fields])
        if current_key not in states_listed_by_key:
            states_listed_by_key.update({
                current_key : {
                                    "states": {state: False for state in state_list}, # we'll declare that data with undimmed states don't need to be articulated
                                    "other_data": {field: row[field] for field in non_key_fields}
                                }
            })
        states_listed_by_key[current_key]["states"][row[state_attr_name]] = True

    # for each key, for each state missing, insert a row into the data with all zeroes
    for key, key_data in states_listed_by_key.items():
        for state, state_is_listed in key_data["states"].items():
            if not state_is_listed:
                new_row = {
                    **{key_fields[field_pos]: key[field_pos] for field_pos in range(len(key_fields))},
                    **key_data["other_data"],
                    **{state_attr_name: state},
                    **{f"Y{year}": 0 for year in time_series}
                }
            
                # re-ordering to put "geo_ref" after "ghg_category"
                reordered_new_row = {}
                for k, v in new_row.items():
                    reordered_new_row[k] = v
                    if k == "ghg_category":
                        reordered_new_row[state_attr_name] = new_row[state_attr_name]
                data.append(reordered_new_row)

    undimmed_state_data_count = len([1 for row in data if row[state_attr_name] not in state_list])
    input_state_articulation_rate = str(input_data_length / (len(data) - undimmed_state_data_count) * 100) # we'll declare that data with undimmed states don't need to be articulated
    # some of this information is incorrect (expected # of rows, articulation rate). Maybe go over with Marty if helpful
    print("=================== State Padding Report ===================")
    print(f"    Key fields:                          {key_fields}")
    print(f"    Distinct keys found:                 {len(states_listed_by_key.keys())}")
    print(f"    Expected number of rows:             {len(states_listed_by_key.keys()) * len(state_list) + undimmed_state_data_count}")
    print(f"    Input number of rows:                {input_data_length}")
    print(f"    # of input rows w/ undimmed states:  {undimmed_state_data_count}")
    print(f"    Number of rows added:                {len(data) - input_data_length}") 
    print(f"    Input state articulation rate:       {input_state_articulation_rate[:input_state_articulation_rate.index('.') + 3] + '%'}")
    print("============================================================")
    return


# ============================================================ MISC HANDLERS ============================================================================

def handle_scriptless_refine_request(pub_year_id: int, layer_id: int, this_job: Job):

    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )
    results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])

    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)
    return helpers.transpose_json_to_portrait(results, time_series)


 # ============================================================ RETURN ALL ==============================================================================

def handle_em_sta_all_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    # execute the query
    # the query response format is: [key_id TEXT, sector TEXT, subsector TEXT, category TEXT, sub_category_1 TEXT, 
                                   # sub_category_2 TEXT, sub_category_3 TEXT, sub_category_4 TEXT, sub_category_5 TEXT, 
                                   # carbon_pool TEXT, fuel1 TEXT, fuel2 TEXT, geo_ref TEXT, exclude TEXT, 
                                   # crt_code TEXT, id TEXT, cbi_activity TEXT, units TEXT, GHG TEXT, ghg_category TEXT, year INTEGER, weighted_quantity NUMERIC]
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )
    results = pub_queries.call_database_query_function(db_constants.PUBLICATION_QUERY_FUNCTIONS["EM_STA_ALL_SNAPSHOT"], pub_year_id, layer_id)
    
    if results is None:
        return None

    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )
    # translate the results from portrait to landscape
    landscape_results = []
    results_row = 0
    reporting_year = db_methods.fetch_reporting_year(pub_year_id)
    max_time_series = db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    while results_row < len(results):
        # initialize the current landscape row of data with the emission key information and first reporting year
        try:
            current_row_of_data = { 
                                    "data_key": "", #placeholder (so it can be the first attribute of the row object)
                                    "sector": sector_mappings[results[results_row][1]], 
                                    "subsector": subsector_mappings[results[results_row][2]], 
                                    "category": category_mappings[results[results_row][3]], 
                                    "sub_category_1": results[results_row][4], 
                                    "sub_category_2": results[results_row][5], 
                                    "sub_category_3": results[results_row][6], 
                                    "sub_category_4": results[results_row][7], 
                                    "sub_category_5": results[results_row][8], 
                                    "carbon_pool": results[results_row][9], 
                                    "fuel1": fuel_mappings[results[results_row][10]],
                                    "fuel2": fuel_mappings[results[results_row][11]],
                                    "ghg": ghg_mappings[results[results_row][12]], 
                                    "ghg_category": results[results_row][13], 
                                    "geo_ref": results[results_row][14], 
                                    "exclude": results[results_row][15], 
                                    "crt_code": results[results_row][16], 
                                    "id": results[results_row][17], 
                                    "cbi_activity": results[results_row][18], 
                                    "units": results[results_row][19], 
                                    f"Y{db_constants.EARLIEST_REPORTING_YEAR}": float(results[results_row][21])
                                }        
        except KeyError as error: 
            print(f"skipping input row {results_row}. KeyError on {error}.")
        data_key_field_positions = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19] # corresponds to the results input, not the to raw_data being constructed
        data_key2_field_positions = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 19] # corresponds to the results input, not the to raw_data being constructed
        current_data_key = generate_data_key(results[results_row], data_key_field_positions) # can't use 'emissions_uid' because state is included as input to emissions_uid's hash function.
        current_year = db_constants.EARLIEST_REPORTING_YEAR + 1
        results_row += 1

        # add the remaining year quantities for this landscape row of data
        while results_row < len(results) and generate_data_key(results[results_row], data_key_field_positions) == current_data_key and current_year <= max_time_series:
            current_row_of_data.update({f"Y{current_year}": float(results[results_row][21])})
            current_year += 1
            results_row += 1
        current_row_of_data.update({"data_key": current_data_key})
        if current_year != max_time_series + 1: # some error logging that should never happen anyway now
            helpers.tprint(f"years going too high! {current_year} row: {results_row}")
        landscape_results.append(current_row_of_data)

    print("resulting format:::")
    for attribute in landscape_results[0]:
        print(f"({landscape_results[0][attribute]}, {type(landscape_results[0][attribute])})")

    return landscape_results

def handle_em_nat_all_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    return handle_em_sta_all_snapshot_request(pub_year_id, layer_id, this_job)


def handle_em_sta_all_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    """refines the data returned by handle_em_sta_all_snapshot_request"""
    
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )
    # results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])
    results = pub_queries.call_database_query_function(db_constants.PUBLICATION_QUERY_FUNCTIONS["EM_STA_ALL_REFINED"], this_job.misc_info["Publication Object ID"])[0][0]
    aggregated_ghg_chemicals = pub_queries.fetch_aggregated_ghg_chemicals()
    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)
    # refine the results
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )
    
    # this field set does not include the geo_ref because all keys are expanded by geo_ref; including it here would square it
    # add any new output fields to this list
    data_key_field_names = [
        "sector", "subsector", "category", "sub_category_1", "sub_category_2", "sub_category_3",
        "sub_category_4", "sub_category_5", "carbon_pool", "fuel1", "fuel2", "ghg", "ghg_category",
        "exclude", "crt_code", "id", "cbi_activity", "units"
    ]

    # just used for re-ordering
    geo_key_fields = []
    for field in data_key_field_names:
        geo_key_fields.append(field)
        if field == "fuel2":
            geo_key_fields.append("geo_ref")
    
    # aggregated_ghg_categories = list(set(aggregated_ghg_chemicals.values())) # dict that maps aggregated chemicals to their aggregation categories
    # # {data_key: aggregated_year_quantities_dict}
    # aggregated_rows_namespace = {}
    # for category in aggregated_ghg_categories: 
    #     exec(f"{category}_rows = " + "{}", aggregated_rows_namespace)
    
    # refined_results = []
    # results_row = 0
    # while results_row < len(results):
    #     current_data_key = (results[results_row]["data_key"])

    #     # if this row belongs to an aggregated GHG category
    #     if results[results_row]["ghg"] in aggregated_ghg_chemicals and results[results_row]["category"] == "Substitution of Ozone Depleting Substances":
    #         current_category_data = aggregated_rows_namespace.get(f"{aggregated_ghg_chemicals[results[results_row]['ghg']]}_rows")
    #         is_first_of_key = current_data_key not in current_category_data
    #         # aggregate rows of same data_key and category together. Store in current_category_data
    #         year_quantities_data = {f"Y{year}": 0 for year in time_series} if is_first_of_key else current_category_data[current_data_key] # load the current aggregates if its not the first key
    #         year = db_constants.EARLIEST_REPORTING_YEAR
    #         for year in time_series: 
    #             year_quantities_data[f"Y{year}"] += results[results_row][f"Y{year}"] # add current aggregate if its not the first key
    #         current_category_data.update({current_data_key: {**{field: results[results_row][field] for field in geo_key_fields}, **year_quantities_data}})
    #     else: # if this row does not belong to an aggregated row_quantity, add it to the output as is
    #         refined_results.append(results[results_row])
    #     results_row += 1

    # # add the aggregated data rows at the end, with the required adjustments
    # for category in aggregated_ghg_categories: 
    #     current_category_data = aggregated_rows_namespace.get(f"{category}_rows")
    #     if len(current_category_data.keys()) > 0: # only add non-zero aggregations
    #         current_category_length = 0
    #         for data_key, column_values in current_category_data.items():
    #             current_category_length += 1
    #             # combine the emissions key values and aggregated year quantity values into one dictionary row 
    #             refined_row = {**{"data_key": data_key}, **column_values}
    #             refined_row["ghg"] = f"Other {category}"
    #             refined_results.append(refined_row)

    #instead of all that ^^^, just this vvv
    refined_results = results


    # add zero valued rows for all missing states in each included key
    if this_job.misc_info["Script Name"] == "EM_Sta_All_Refined": # skip this step for em_NAT_all_refined
        pad_data_with_zeroes_for_missing_states_per_data_key(refined_results, time_series, key_fields=data_key_field_names)

    return refined_results


def handle_em_nat_all_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    return handle_em_sta_all_refined_request(pub_year_id, layer_id, this_job)


 # ============================================================ GROUP BY SECTOR ==============================================================================

def handle_em_sta_sector_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    """similar to handle_em_sta_all_snapshot_request, except group by sector and ghg_category; also exclude HFE quantities
    Output record structure: (sector, ghg_category_name, Y1990_quantity, Y1991_quantity, ...)"""
    
    # execute the query
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )
    results = pub_queries.call_database_query_function(db_constants.PUBLICATION_QUERY_FUNCTIONS["EM_STA_SECTOR_SNAPSHOT"], pub_year_id, layer_id)
    
    if results is None:
        return None

    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )
    # translate the results from portrait to landscape
    landscape_results = []
    results_row = 0
    while results_row < len(results):
        # initialize the current landscape row of data with the emission key information and first reporting year
        current_group_id = results[results_row][0:2]
        current_row_of_data = { "sector": sector_mappings[results[results_row][0]], 
                                "ghg_category_name": results[results_row][1], 
                                "Y1990": float(results[results_row][3])}

        current_year = db_constants.EARLIEST_REPORTING_YEAR + 1
        results_row += 1
        # add the remaining year quantities for this landscape row of data
        while results_row < len(results) and results[results_row][0:2] == current_group_id:
            current_row_of_data.update({f"Y{current_year}": float(results[results_row][3])})
            current_year += 1
            results_row += 1
        landscape_results.append(current_row_of_data)

    return landscape_results


def handle_em_nat_sector_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    return handle_em_sta_sector_snapshot_request(pub_year_id, layer_id, this_job)


# ============================================== ECONSECT =================================================================================

def handle_em_sta_econsect_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )

    results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])
    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)

    # refine the results
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )

    # pivot data to landscape
    refined_results = helpers.transpose_json_to_landscape(results, time_series)

    attribute_order = ["ViewName", "Row Number", "Economic Sector", "Economic SubSector", "GHG"] # State and year quantities are added in the append() operation
    refined_results = []
    for result in results: 
        # populate missing column values with NULL
        for column in attribute_order: 
            if column not in result:
                result.update({column: None})
        # reorder attributes for each row
        refined_results.append({**{attr: result[attr] for attr in attribute_order}, **{f'Y{year}': (0 if f'Y{year}' not in result else result[f'Y{year}']) for year in time_series}})

    return refined_results


def handle_em_nat_econsect_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    return handle_em_sta_econsect_refined_request(pub_year_id, layer_id, this_job)


def handle_em_sta_econsect_byst_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )

    results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])
    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)
    
    # refine the results
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )

    # add missing states for each ROW (this means multiplying the set by len(states_list) [all quantities of added rows are zeros])
    # also reorders the attributes in each data row consistently
    attribute_order = ["Row_Title", "Aggregates To", "Row_Subtitle", "Formula", "Gas", "State"] # State and year quantities are added in the append() operation
    refined_results = []
    for result in results: 
        # populate missing column values with NULL
        for column in attribute_order:
            if column not in result:
                result.update({column: None}) 
        # reorder attributes for each row
        refined_results.append({**{attr: result[attr] for attr in attribute_order}, **{f'Y{year}': (0 if f'Y{year}' not in result else result[f'Y{year}']) for year in time_series}})
    # add missing states for each unique key defined by the non-year quantity, non-state columns
    pad_data_with_zeroes_for_missing_states_per_data_key(refined_results, time_series, ["Row_Title", "Aggregates To", "Row_Subtitle", "Formula", "Gas"], [], "State")
    return refined_results


# ===================================== SUBSECTOR ================================================================================

def handle_em_sta_subsector_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )

    results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])
    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)

    # refine the results
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )

    # pivot data to landscape
    refined_results = helpers.transpose_json_to_landscape(results, time_series)

    attribute_order = ["ViewName", "Row Number", "Source", "GHG"] # State and year quantities are added in the append() operation
    refined_results = []
    for result in results: 
        # populate missing column values with NULL
        for column in attribute_order: 
            if column not in result:
                result.update({column: None})
        # reorder attributes for each row
        refined_results.append({**{attr: result[attr] for attr in attribute_order}, **{f'Y{year}': (0 if f'Y{year}' not in result else result[f'Y{year}']) for year in time_series}})

    return refined_results


def handle_em_nat_subsector_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    return handle_em_sta_subsector_refined_request(pub_year_id, layer_id, this_job)


def handle_em_sta_subsector_byst_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "FETCHING_DATA",
    )

    results = pub_queries.fetch_publication_refine_script_data(this_job.misc_info["Publication Object ID"])
    time_series = db_methods.get_time_series_by_pub_year_id(pub_year_id)
    aggregated_ghg_chemicals = pub_queries.fetch_aggregated_ghg_chemicals(ghg_name_select="ghg_code")
    

    # refine the results
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "PROCESSING_QUERY_RESULTS",
    )

    # redact substitution of ozone depleting substances
    attribute_order = ["Row_Title", "Aggregates To", "Row_Subtitle", "Formula", "Gas", "State"] # State and year quantities are added in the append() operation
    refined_results = []
    for result in results: 
        # populate missing column values with NULL
        for column in attribute_order: 
            if column not in result:
                result.update({column: None})
        # redact formulas of HFC, PFC, and HFE chemicals -> set Formula to NULL
        if result["Formula"]: 
            for chemical in aggregated_ghg_chemicals:
                if chemical in result["Formula"]:
                    result["Formula"] = None
                    break
        # reorder attributes for each row
        refined_results.append({**{attr: result[attr] for attr in attribute_order}, **{f'Y{year}': (0 if f'Y{year}' not in result else result[f'Y{year}']) for year in time_series}})
    pad_data_with_zeroes_for_missing_states_per_data_key(refined_results, time_series, ["Row_Title", "Aggregates To", "Row_Subtitle", "Formula", "Gas"], [], "State")

    return refined_results


# ================================================ POWER USERS SNAPSHOT =============================================================================


def handle_em_sta_powerusers_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "WRITING_TO_DATABASE",
    )
    pub_queries.call_database_query_function(db_constants.PUBLICATION_QUERY_FUNCTIONS["EM_STA_POWERUSERS_SNAPSHOT"], this_job.misc_info["Publication Object ID"], pub_year_id, layer_id)[0][0]
    return False

def handle_em_nat_powerusers_snapshot_request(pub_year_id: int, layer_id: int, this_job: Job):
    """Do the same as the state version"""
    return handle_em_sta_powerusers_snapshot_request(pub_year_id, layer_id, this_job)


# ================================================ POWER USERS REFINED =============================================================================


def handle_em_sta_powerusers_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "WRITING_TO_DATABASE",
    )
    return pub_queries.copy_publication_raw_data_to_refined(this_job.misc_info["Publication Object ID"]) # just return the raw data for now.


def handle_em_nat_powerusers_refined_request(pub_year_id: int, layer_id: int, this_job: Job):
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "WRITING_TO_DATABASE",
    )
    return pub_queries.copy_publication_raw_data_to_refined(this_job.misc_info["Publication Object ID"]) # just return the raw data for now.


# ================================================ UNCATEGORIZED MISC =============================================================================

def handle_act_sta_popgdp_request(pub_year_id: int, layer_id: int, this_job: Job):
    # fetch the raw data from the selected publication object
    this_job.post_event(
        "PUBLICATION_PROCESSING",
        "WRITING_TO_DATABASE",
    )
    return pub_queries.call_database_query_function(db_constants.PUBLICATION_QUERY_FUNCTIONS["ACT_STA_POPGDP"], pub_year_id, layer_id)[0][0]


def handle_publication_processing_request(pub_object_id: int,   action: str,   user_id: int):
    try:
        # fetch the relevant data product information from the database based on the input arguments
        helpers.tprint("Fetching the selected data product information...")
        
        data_product_info = pub_queries.fetch_publication_data_product_info(pub_object_id, action) 
        if data_product_info is None:
            return {"result": f"There is no publication object with ID: {pub_object_id}"}
        selected_script_name, reporting_year, layer_id = data_product_info

        this_job = Job(
            job_constants.PUBLICATION_PROCESSING_NAME,
            job_constants.PUBLICATION_PROCESSING_DESC,
            reporting_year,
            layer_id,
            user_id,
            misc_info = {"Publication Object ID": pub_object_id, "Action": action}
        )

        if selected_script_name == "na" and action == db_constants.PUBLICATION_ACTIONS["PREPARE"]: # abort if there is no script
            return {"result": f"There is no '{action}' action associated with publication object with ID: {pub_object_id}"}
        if selected_script_name == "na" and action == db_constants.PUBLICATION_ACTIONS["REFINE"]: # simply copy and sort columns for scriptless refine calls
            selected_script_name = "Scriptless_Refine"

        # get pub_year_id
        pub_year_id = db_methods.fetch_pub_year_id(reporting_year)
        helpers.tprint(f"Data product information found: Script Name: {selected_script_name}, Reporting Year/Pub Year ID: {reporting_year}/{pub_year_id}, Layer ID: {layer_id}")

        # run the script via dynamic reference (function must be named according to selected_script_name)
        helpers.tprint(f"Running the selected script...")
        
        expected_function_name = f"handle_{selected_script_name.lower()}_request"
        this_job.misc_info.update({"Script Name": selected_script_name})
        if hasattr(sys.modules[__name__], expected_function_name):
            script_function = getattr(sys.modules[__name__], expected_function_name)
            results = script_function(pub_year_id, layer_id, this_job) # will ignore action_params argument if it isn't required by the selected script
        else: 
            result_msg = f"There is no defined handler for the '{action}' action for the publication object with ID: {pub_object_id}"
            helpers.tprint(result_msg)
            return {"result": result_msg}

        if (isinstance(results, list) or isinstance(results, tuple)) and len(results) > 0:
            helpers.tprint(f"Script completed. There are {len(results)} results. The first result is: \n{results[0]}")
            helpers.tprint(f"And the last result is: \n{results[-1]}")

        # write the result of the handler to the database accordingly
        helpers.tprint("Updating the database...")
        this_job.post_event(
            "PUBLICATION_PROCESSING",
            "WRITING_TO_DATABASE",
        )

        if action == db_constants.PUBLICATION_ACTIONS["REFINE"]:
            table_name = pub_queries.fetch_publication_raw_tablename(pub_object_id).replace("Import", "Refined").replace("Snapshot", "Refined")
        else:
            table_name = f'{selected_script_name}_{reporting_year}_{helpers.get_timestamp("%m%d%Y")}'
        
        skip_updating_data_cell = (isinstance(results, bool) and results == False) # don't change the data cell here when results is False (this would be completed by the PostgreSQL query function)
        if results == False:
            results = []
        pub_queries.update_data_product_result_in_database(pub_object_id, action, results, table_name, user_id, skip_updating_data_cell)
        
        helpers.tprint("Done.")
        this_job.update_status("COMPLETE")

        return jsonify({"result": "Publication object action completed."}), 200

    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        if "this_job" in locals(): 
            this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500