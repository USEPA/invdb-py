import chalicelib.src.query_engine.jobs.execute_simple_query.queries as queries
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os

<<<<<<< HEAD

query_formulas_info = db_methods.fetch_query_formula_name_mappings(by_query_formula_id=True, include_parameters=True)


def prepare_query_parameters(query_info: list[tuple[str, int, dict]], gwp: str=None):
=======
state_list = db_methods.fetch_dim_state_list()

query_formulas_info = db_methods.fetch_query_formula_name_mappings(
    by_query_formula_id=True, include_parameters=True
)


def prepare_query_parameters(query_info: list[tuple[str, int, dict]], gwp: str = None):
>>>>>>> gitlab/develop
    prepared_queries_info = []
    invalid_queries_row_ids = []

    for query in query_info:
<<<<<<< HEAD
        if query[1] not in query_formulas_info: # mark unrecognized query_formula_ids as invalid 
            invalid_queries_row_ids.append(query[0])
            continue
        expected_parameter_order = [param.strip() for param in query_formulas_info[query[1]][1].upper().split(",")] # parse the expected parameter order list
        parameter_values = {key.upper(): value for key, value in query[2].items()} # convert the argument keys to all upper case to match the expected parameter casing
        try:
            # map the arguments to their ordered parameters
            argument_list = tuple([parameter_values[parameter] for parameter in expected_parameter_order])
            if gwp is not None: 
=======
        if (
            query[1] not in query_formulas_info
        ):  # mark unrecognized query_formula_ids as invalid
            invalid_queries_row_ids.append(query[0])
            continue
        expected_parameter_order = [
            param.strip()
            for param in query_formulas_info[query[1]][1].upper().split(",")
        ]  # parse the expected parameter order list
        parameter_values = {
            key.upper(): value for key, value in query[2].items()
        }  # convert the argument keys to all upper case to match the expected parameter casing
        try:
            # map the arguments to their ordered parameters
            argument_list = tuple(
                [parameter_values[parameter] for parameter in expected_parameter_order]
            )
            if gwp is not None:
>>>>>>> gitlab/develop
                argument_list = argument_list + (gwp,)
            # generate the tuple that can be passed to the query executor
            prepared_queries_info.append(
                (
<<<<<<< HEAD
                    query[0],                                           # custom_query_id_str
                    query_formulas_info[query[1]][0],                   # query_formula_id
                    argument_list                                       # formula parameter values tuple
                ) 
            )
            # sort the current list by the formula_prefix
        except KeyError: 
            invalid_queries_row_ids.append(query[0]) # just the query_id

    return prepared_queries_info, invalid_queries_row_ids

def format_response_object(results: list[tuple[str, int, float]], invalid_queries_row_ids: list[int], reporting_year: int) -> dict:
    '''translate query results data to the format expected by the query_engine.
       input: 
          results: list of tuples: [0]: custom_query_id_str, [1]: reporting_year, [2]: emissions value
          invalid_queries_row_ids: list of custom_query_id_strs where valid query logic couldn't be determined'''
    formatted_results = {}

    # structure results for the valid simple queries
    for result in results: 
        if result[0] not in formatted_results:
            formatted_results[result[0]] = {str(db_methods.fetch_year_id(result[1])): float(result[2])}
        else: 
            formatted_results[result[0]].update({str(db_methods.fetch_year_id(result[1])): float(result[2])})
    
    # append results with null values for all quantities of query_ids that didn't express a valid simple query
    max_year_id = db_methods.fetch_year_id(db_methods.fetch_max_time_series_by_reporting_year(reporting_year))
    for report_row_id in invalid_queries_row_ids:
        formatted_results[str(report_row_id)] = {**{str(year_id): 0 for year_id in range(1, max_year_id + 1)}}
    
    return formatted_results


def execute_simple_query(query_info: list[tuple[str, int, dict]], reporting_year: int, layer_id: int, gwp: str=None) -> dict:
    '''manages the entire simple query execution process as called by the query engine. 
       input: ids: list of queries, where a query is defined by a tuple with the following 
       elements: [0]: any_custom_id_str, [1]: query_formula_id, [2]: query_parameters_json_as_dict
       supports single or multiple report rows as input.
       supports any mixture of query_types of query_class SIMPLE
       If report_type_id == 1: return an emissions simple query
       If report_type_id == 2: return a QC simple query'''
    if isinstance(query_info, list) and len(query_info) == 0:
        return format_response_object({}, [query[0] for query in query_info], reporting_year)

    # prepare the queries
    prepared_queries_info, invalid_queries_row_ids_from_prep = prepare_query_parameters(query_info, gwp)
    # give warning about any invalid queries due to missing parameter values
    if len(invalid_queries_row_ids_from_prep) > 0:
        helpers.tprint(f"handle_load_online_report_request(): WARNING: The following queries have one or more missing parameter values, and thus cannot be executed: {invalid_queries_row_ids_from_prep}")
=======
                    query[0],  # custom_query_id_str
                    query_formulas_info[query[1]][0],  # query_formula_id
                    argument_list,  # formula parameter values tuple
                )
            )
            # sort the current list by the formula_prefix
        except KeyError:
            invalid_queries_row_ids.append(query[0])  # just the query_id

    return prepared_queries_info, invalid_queries_row_ids


def format_response_object(
    results: list[tuple[str, int, float]],
    invalid_queries_row_ids: list[int],
    reporting_year: int,
    all_queries_row_ids: list[int],
    is_group_by_state: bool = False,
) -> dict:
    """translate query results data to the format expected by the query_engine.
    input:
       results: list of tuples: [0]: custom_query_id_str, [1]: reporting_year, [2]: emissions value
       invalid_queries_row_ids: list of custom_query_id_strs where valid query logic couldn't be determined
    """
    formatted_results = {}

    max_year_id = db_methods.fetch_year_id(
        db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    )

    if is_group_by_state:
        # structure results for the valid simple queries
        # If query results don't always have data for all states then
        # this could lead to inconsistent data across states.
        # To avoid this, we initialize all states with all queries.
        # This ensures consistency across states.
        formatted_results = {state: {} for state in state_list}

        for result in results:
            key = result[0]  # gets query id ex: SQ175, SQ175_QC etc
            year_id = str(db_methods.fetch_year_id(result[1]))
            value = float(result[2])

            state = result[3] if result[3] is not None else "null"
            # if state value is not from state_list we ignore it
            if state in formatted_results:
                if key not in formatted_results[state]:
                    formatted_results[state][key] = {year_id: value}
                else:
                    formatted_results[state][key].update({year_id: value})

        # append results with null values for all quantities of query_ids that didn't express a valid simple query
        for report_row_id in invalid_queries_row_ids:
            for state in formatted_results:
                formatted_results[state][str(report_row_id)] = {
                    str(year_id): 0 for year_id in range(1, max_year_id + 1)
                }

        # Check if all query IDs are present in the formatted results for each state
        for state, query_data in formatted_results.items():
            missing_ids = set(all_queries_row_ids) - set(query_data.keys())
            if missing_ids:
                # Add missing query IDs with zero values for all years
                for query_id in missing_ids:
                    formatted_results[state][query_id] = {
                        str(year_id): 0 for year_id in range(1, max_year_id + 1)
                    }

    else:
        # structure results for the valid simple queries
        # add 'null' state to formatted_results dictionary
        formatted_results = {"null": {}}
        for result in results:
            key = result[0]  # gets query id ex: SQ175, SQ175_QC etc
            year_id = str(db_methods.fetch_year_id(result[1]))
            value = float(result[2])
            if key not in formatted_results["null"]:
                formatted_results["null"][key] = {year_id: value}
            else:
                formatted_results["null"][key].update({year_id: value})

        # append results with null values for all quantities of query_ids that didn't express a valid simple query
        for report_row_id in invalid_queries_row_ids:
            formatted_results["null"][str(report_row_id)] = {
                str(year_id): 0 for year_id in range(1, max_year_id + 1)
            }

    # # structure results for the valid simple queries
    # for result in results:
    #     key = result[0]  # gets query id ex: SQ175, SQ175_QC etc
    #     year_id = str(db_methods.fetch_year_id(result[1]))
    #     value = float(result[2])
    #     if results_grouped_by_state:
    #         # If query results don't always have data for all states then
    #         # this could lead to inconsistent data across states.
    #         # To avoid this, we initialize all states with all queries.
    #         # This ensures consistency across states. So, we'll create a dictionary with all states and all
    #         formatted_results = {state: {} for state in state_list}
    #         state = result[3] if result[3] is not None else "null"
    #         # if state not in formatted_results:
    #         #     formatted_results[state] = {key: {year_id: value}}
    #         # else:
    #         if state in formatted_results:
    #             if key not in formatted_results[state]:
    #                 formatted_results[state][key] = {year_id: value}
    #             else:
    #                 formatted_results[state][key].update({year_id: value})
    #     else:
    #         if key not in formatted_results:
    #             formatted_results[key] = {year_id: value}
    #         else:
    #             formatted_results[key].update({year_id: value})

    # append results with null values for all quantities of query_ids that didn't express a valid simple query
    # max_year_id = db_methods.fetch_year_id(
    #     db_methods.fetch_max_time_series_by_reporting_year(reporting_year)
    # )
    # for report_row_id in invalid_queries_row_ids:
    #     if results_grouped_by_state:
    #         for state in formatted_results:
    #             formatted_results[state][str(report_row_id)] = {
    #                 str(year_id): 0 for year_id in range(1, max_year_id + 1)
    #             }
    #     else:
    #         formatted_results[str(report_row_id)] = {
    #             str(year_id): 0 for year_id in range(1, max_year_id + 1)
    #         }

    return formatted_results


def execute_simple_query(
    query_info: list[tuple[str, int, dict]],
    reporting_year: int,
    layer_id: int,
    is_group_by_state: bool = False,
    gwp: str = None,
) -> dict:
    """manages the entire simple query execution process as called by the query engine.
    input: ids: list of queries, where a query is defined by a tuple with the following
    elements: [0]: any_custom_id_str, [1]: query_formula_id, [2]: query_parameters_json_as_dict
    supports single or multiple report rows as input.
    supports any mixture of query_types of query_class SIMPLE
    If report_type_id == 1: return an emissions simple query
    If report_type_id == 2: return a QC simple query"""
    all_queries_row_ids = [query[0] for query in query_info]
    if isinstance(query_info, list) and len(query_info) == 0:
        return format_response_object(
            {},
            [query[0] for query in query_info],
            reporting_year,
            all_queries_row_ids,
            is_group_by_state,
        )

    # prepare the queries
    prepared_queries_info, invalid_queries_row_ids_from_prep = prepare_query_parameters(
        query_info, gwp
    )
    # give warning about any invalid queries due to missing parameter values
    if len(invalid_queries_row_ids_from_prep) > 0:
        helpers.tprint(
            f"handle_load_online_report_request(): WARNING: The following queries have one or more missing parameter values, and thus cannot be executed: {invalid_queries_row_ids_from_prep}"
        )
>>>>>>> gitlab/develop

    # process the queries (in batches if needed)
    results = []
    all_invalid_queries_row_ids = []
    all_invalid_queries_row_ids += invalid_queries_row_ids_from_prep
    batch_size = db_constants.QUERIES_PER_REQUEST
    query_count = len(prepared_queries_info)
    batch_count = math.ceil(query_count / batch_size)
    batch_number = 1

    if invdb_globals.allow_multithreading:
<<<<<<< HEAD
    #=================== multi-threaded version ========================
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for i in range(0, query_count, batch_size):
                batch = prepared_queries_info[i : i+batch_size]
                future = executor.submit(
                    queries.process_simple_query_batch,
                    batch, 
                    reporting_year, 
                    layer_id
=======
        # =================== multi-threaded version ========================
        with ThreadPoolExecutor(max_workers=os.cpu_count()) as executor:
            futures = []
            for i in range(0, query_count, batch_size):
                batch = prepared_queries_info[i : i + batch_size]
                future = executor.submit(
                    queries.process_simple_query_batch, batch, reporting_year, layer_id
>>>>>>> gitlab/develop
                )
                futures.append(future)

            for future in as_completed(futures):
                results_this_batch, invalid_queries_this_batch = future.result()
                results += results_this_batch
                all_invalid_queries_row_ids += invalid_queries_this_batch

            executor.shutdown(wait=True)
<<<<<<< HEAD
    #=================== single-threaded version ========================
    else:
        # process each full batch
        for i in range(0, query_count, batch_size):
            batch = prepared_queries_info[i : i+batch_size]
            if len(batch) < batch_size: # for the final partial batch, if needed
                batch = prepared_queries_info[-(len(prepared_queries_info) % batch_size):]
            helpers.tprint(f"Processing simple query batch {batch_number} of {batch_count}...")
            results_this_batch, invalid_queries_this_batch = queries.process_simple_query_batch(batch, reporting_year, layer_id)
            results += results_this_batch
            all_invalid_queries_row_ids += invalid_queries_this_batch
            batch_number += 1
    #====================================================================

    return format_response_object(results, all_invalid_queries_row_ids, reporting_year)


def handle_simple_query_request(queries: list[tuple[int, dict]], reporting_year: int, layer_id: int, user_id: int) -> dict:
    '''API endpoint logic that exposes the execute_simple_query() function above'''
    query_info = [(f"Query {index + 1}", query[0], query[1]) for index, query in enumerate(queries)]
    result = execute_simple_query(query_info, reporting_year, layer_id)
    return result
=======
    # =================== single-threaded version ========================
    else:
        # process each full batch
        for i in range(0, query_count, batch_size):
            batch = prepared_queries_info[i : i + batch_size]
            if len(batch) < batch_size:  # for the final partial batch, if needed
                batch = prepared_queries_info[
                    -(len(prepared_queries_info) % batch_size) :
                ]
            helpers.tprint(
                f"Processing simple query batch {batch_number} of {batch_count}..."
            )
            results_this_batch, invalid_queries_this_batch = (
                queries.process_simple_query_batch(batch, reporting_year, layer_id)
            )
            results += results_this_batch
            all_invalid_queries_row_ids += invalid_queries_this_batch
            batch_number += 1
    # ====================================================================

    return format_response_object(
        results,
        all_invalid_queries_row_ids,
        reporting_year,
        all_queries_row_ids,
        is_group_by_state,
    )


def handle_simple_query_request(
    queries: list[tuple[int, dict]], reporting_year: int, layer_id: int, user_id: int
) -> dict:
    """API endpoint logic that exposes the execute_simple_query() function above"""
    query_info = [
        (f"Query {index + 1}", query[0], query[1])
        for index, query in enumerate(queries)
    ]
    result = execute_simple_query(query_info, reporting_year, layer_id)
    return result
>>>>>>> gitlab/develop
