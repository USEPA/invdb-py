import chalicelib.src.reports.jobs.load_online_report.queries as load_online_report_queries
# from chalicelib.src.reports.models.OnlineReportQuery import OnlineReportQuery
import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.jobs.constants as job_constants
from chalicelib.src.jobs.models.Job import Job as Job_Class
from chalicelib.src.query_engine.methods import execute_queries_by_class
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.general.helpers as helpers
from concurrent.futures import ThreadPoolExecutor
from flask import jsonify
import json


def split_queries_by_class_and_type(report_queries_info: list[tuple]) -> dict:
    '''takes the full list of queries and returns a 2-level nested dict. The first level 
    separates keys by query class (simple v.s. complex). The second level separates the queries of each class by their type (emissions v.s. QC)'''
    # first sort by the classes
    distinct_classes = sorted(list(set([(query[5], query[7]) for query in report_queries_info])), key=lambda x: x[1]) # sort the classes by their priority
    queries_by_class = {class_name: [query for query in report_queries_info if query[5] == class_name] for class_name, priority in distinct_classes}

    # then sort the types within those class lists
    queries_by_class_and_type = {}
    for class_name in queries_by_class:
        distinct_types = sorted(list(set([(query[6], query[7]) for query in queries_by_class[class_name]])), key=lambda x: x[1]) # sort the types by their priority
        queries_by_class_and_type[class_name] = {type_name: [query for query in queries_by_class[class_name] if query[6] == type_name] for type_name, priority in distinct_types}
        
    return queries_by_class_and_type


def prepare_queries_info_for_processing(report_queries_by_class_and_type: dict) -> tuple:
    '''process the query_info object to hand it to the batch query processor:'''
    # setup the structure for the return dict
    prepared_queries_by_class_and_type = {}
    for class_name in report_queries_by_class_and_type:
        prepared_queries_by_class_and_type[class_name] = {}
        for type_name in report_queries_by_class_and_type[class_name]:
            prepared_queries_by_class_and_type[class_name][type_name] = [] #clear the copied contents of the curent query list
    
    # iterate through all the queries and prepare them so query_engine processing
    for class_name in report_queries_by_class_and_type:
        for type_name in report_queries_by_class_and_type[class_name]:
            for query in report_queries_by_class_and_type[class_name][type_name]:
                prepared_queries_by_class_and_type[class_name][type_name].append(
                    (
                        f"{query[0]}__{type_name}", # custom_query_id_str
                        query[1],                   # query_formula_id
                        query[2]                    # formula and possibly other information in a dict
                    ) 
                )

    # sort each simple query list by the formula_prefix
    if 'SIMPLE' in report_queries_by_class_and_type:
        for type_name in report_queries_by_class_and_type['SIMPLE']:
            prepared_queries_by_class_and_type['SIMPLE'][type_name] = sorted(prepared_queries_by_class_and_type['SIMPLE'][type_name], key=lambda x: x[1]) 

    return prepared_queries_by_class_and_type


def execute_report_queries_with_query_engine(prepared_report_queries_by_class_and_type: list[tuple], reporting_year: int, layer_id: int, gwp: str=None) -> dict:
    """takes a list of OnlineReportQuery objects (simple, complex, etc.), passes each query
    to the query_engine, and attaches the results to each object under the `results` attribute."""
    combined_results = {}
    for class_name in prepared_report_queries_by_class_and_type:
        # gather all the queries in the current class
        all_class_queries = []
        for type_name in prepared_report_queries_by_class_and_type[class_name]:
            all_class_queries += prepared_report_queries_by_class_and_type[class_name][type_name]

        # send all those queries to the appropriate query_engine worker and record the results in a list
        results = execute_queries_by_class(all_class_queries, class_name, reporting_year, layer_id, gwp)
        combined_results.update(results)

    return combined_results


def handle_load_online_report_request(report_id: int, report_type_id: int, user_id: int, gwp: str=None):
    """Load online report API logic. Will fetch ALL report queries pertaining to the input online report, 
       split the queries by their classes, and direct those sets of queries to the appropriate query engine executors,
       and then combines the result sets into a single response object."""
    this_job = Job_Class(
        job_constants.LOAD_ONLINE_REPORT_NAME,
        job_constants.LOAD_ONLINE_REPORT_DESC,
        2024,
        1,
        user_id,
        misc_info = {"Report ID": report_id, "Report Type ID": report_type_id}
    )

    try:
        if report_type_id != 1: #only consider GWP column select for type 1 (emissions) report requests
            gwp = None

        helpers.tprint("Fetching queries information...")
        this_job.post_event(
            "LOAD_ONLINE_REPORT",
            "FETCHING_QUERY_INFO"
        )
        report_queries_info = load_online_report_queries.fetch_queries_for_online_report(report_id, report_type_id)

        if len(report_queries_info) == 0:
            helpers.tprint("Done.")
            return jsonify({
                "report_id": report_id,
                "query_results": {}
                }
            ), 400        

        reporting_year = report_queries_info[0][3]
        layer_id = report_queries_info[0][4]

        # wait until rollup tables are up-to-date if not currently
        db_methods.wait_on_rollup_tables_refresh(reporting_year, layer_id)


        # place any queries with parameters = None to a special list so they return all 0s instead of being processed by the query engine
        invalid_query_ids = [f'{query[0]}__{query[6]}' for query in report_queries_info if query[2] is None]
        report_queries_info = list(filter(lambda x: x[2] is not None, report_queries_info))
        
        helpers.tprint("Preparing queries for processing...")
        this_job.post_event(
            "LOAD_ONLINE_REPORT",
            "PREPARING_QUERIES"
        )
        report_queries_by_class_and_type = split_queries_by_class_and_type(report_queries_info)
        
        # prepare the query formula info so it can be passed to the batch processor
        prepared_report_queries_by_class_and_type = prepare_queries_info_for_processing(report_queries_by_class_and_type)
       
        helpers.tprint("Processing queries with query engine...")
        this_job.post_event(
            "LOAD_ONLINE_REPORT",
            "PROCESSING_QUERIES"
        )
        # get the valid query results from the query engine
        report_queries_results = execute_report_queries_with_query_engine(prepared_report_queries_by_class_and_type, reporting_year, layer_id, gwp)
        
        # add the invalid queries' all zero data to the result set
        max_year_id = db_methods.fetch_year_id(db_methods.fetch_max_time_series_by_reporting_year(reporting_year))
        for invalid_query_id in invalid_query_ids:
            report_queries_results.update({invalid_query_id: {str(year_id): 0 for year_id in range(1, max_year_id + 1)}})

        helpers.tprint("Constructing and sending response object...")
        this_job.post_event(
            "LOAD_ONLINE_REPORT",
            "CONSTRUCTING_RESPONSE_OBJECT"
        )

        # construct the response object
        queries_data_obj = {}
        for query_id, query_results in report_queries_results.items():
            report_row_id, query_type = query_id.split("__") # parse the custom_query_id_str into its original components
            if report_row_id not in queries_data_obj:
                queries_data_obj[report_row_id] = {"report_row_id": report_row_id, db_constants.QUERY_TYPES[query_type.upper()]["time_series_name"]: query_results}
            else:
                queries_data_obj[report_row_id].update({db_constants.QUERY_TYPES[query_type.upper()]["time_series_name"]: query_results})

                    
        # convert the data object to a list
        queries_data_obj = list(queries_data_obj.values())

        data_response_object = {
            "report_id": report_id,
            "query_results": queries_data_obj
        }
        helpers.tprint("Done.")
        return jsonify(data_response_object), 200

    except Exception:
        from chalicelib.src.database.methods import get_pgdb_connection
        import traceback
        this_job.update_status("ERROR")
        pgdb_connection = get_pgdb_connection()
        pgdb_connection.rollback()
        traceback_obj = traceback.format_exc()
        helpers.tprint(traceback_obj)
        return jsonify({"traceback": traceback_obj}), 500