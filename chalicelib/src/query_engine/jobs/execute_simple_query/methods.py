import chalicelib.src.query_engine.jobs.execute_simple_query.queries as queries
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
import chalicelib.src.general.globals as invdb_globals
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
import math
import os


query_formulas_info = db_methods.fetch_query_formula_name_mappings(by_query_formula_id=True, include_parameters=True)


def prepare_query_parameters(query_info: list[tuple[str, int, dict]], gwp: str=None):
    prepared_queries_info = []
    invalid_queries_row_ids = []

    for query in query_info:
        if query[1] not in query_formulas_info: # mark unrecognized query_formula_ids as invalid 
            invalid_queries_row_ids.append(query[0])
            continue
        expected_parameter_order = [param.strip() for param in query_formulas_info[query[1]][1].upper().split(",")] # parse the expected parameter order list
        parameter_values = {key.upper(): value for key, value in query[2].items()} # convert the argument keys to all upper case to match the expected parameter casing
        try:
            # map the arguments to their ordered parameters
            argument_list = tuple([parameter_values[parameter] for parameter in expected_parameter_order])
            if gwp is not None: 
                argument_list = argument_list + (gwp,)
            # generate the tuple that can be passed to the query executor
            prepared_queries_info.append(
                (
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

    # process the queries (in batches if needed)
    results = []
    all_invalid_queries_row_ids = []
    all_invalid_queries_row_ids += invalid_queries_row_ids_from_prep
    batch_size = db_constants.QUERIES_PER_REQUEST
    query_count = len(prepared_queries_info)
    batch_count = math.ceil(query_count / batch_size)
    batch_number = 1

    if invdb_globals.allow_multithreading:
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
                )
                futures.append(future)

            for future in as_completed(futures):
                results_this_batch, invalid_queries_this_batch = future.result()
                results += results_this_batch
                all_invalid_queries_row_ids += invalid_queries_this_batch

            executor.shutdown(wait=True)
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