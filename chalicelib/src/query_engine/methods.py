import chalicelib.src.database.methods as db_methods
import chalicelib.src.database.constants as db_constants
from chalicelib.src.query_engine.jobs.execute_simple_query.methods import execute_simple_query
from chalicelib.src.query_engine.jobs.execute_complex_query.methods import execute_complex_query
# import additional query executors here
import json

def get_qe_years_object(reporting_year: int):
    time_series = db_methods.get_time_series_with_ids_by_rptyr(reporting_year)
    if time_series is None:
        raise ValueError(f"No time series found for reporting year: {id}")
    # Initialize the dictionary
    year_dict = {str(year_id): None for year, year_id in time_series}
    return year_dict

def execute_queries_by_class(queries: list[tuple], query_class_name: str, reporting_year: int, layer_id: int, gwp: str=None) -> dict:
    """execute single or batch of report queries by report_row_id, passing the class to select the exector."""
    if query_class_name not in [query_class["name"] for query_class in db_constants.QUERY_CLASSES.values()]:
        raise ValueError("execute_query_by_class(): Error: unknown query class passed")

    if query_class_name == db_constants.QUERY_CLASSES['SIMPLE']['name']:
        query_results = execute_simple_query(queries, reporting_year, layer_id, gwp)
        return ({} if query_results is None else query_results)
    if query_class_name == db_constants.QUERY_CLASSES['COMPLEX']['name']: 
        all_results = {}
        for query_id, _, query_parameters in queries:
            all_results.update({query_id: execute_complex_query(query_parameters, reporting_year, layer_id, gwp)})
        return all_results
    # if query_class_name == db_constants.QUERY_CLASSES['...']['name']: 
        # add additional query class handlers here