import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers


def process_simple_query_batch(query_batch: [tuple], reporting_year: int, layer_id: int) -> [tuple]:
    """based on report processing's process_report_query_batch. 
    INPUT: list of tuples. Each tuple represents a query formula call
    ([0]: custom_query_id_str, 
     [1]: formula_prefix, 
     [2:]: variable number of arguments to pass to the query function.)
    OUTPUT: dict where keys denote the result report_row_id and map to the row 
    of values from 1990 to the max time series to write into Query_Results
    tab of the report"""
    # generate the SQL query statement
    all_query_ids = [str(query[0]) for query in query_batch]
    pub_year_id = db_methods.fetch_pub_year_id(reporting_year)
    SQL_statement = ""
    for query in query_batch:
        SQL_statement += f"""SELECT '{query[0]}' AS query_id_str, *
        FROM ggds_invdb.{query[1]}{helpers.get_sql_list_str([pub_year_id, layer_id] + list(query[2]))}
        UNION ALL\n"""
    SQL_statement = SQL_statement[:-10] # strip the trailing 'UNION ALL\n'
    # SQL_statement += f"ORDER BY report_row_id,{' geo_ref, ' if report_type == report_constants.REPORT_TYPES['STATE'] else ' '} year;" # for when state reports are supported
    SQL_statement += f"ORDER BY query_id_str, year;"
    results = db_methods.get_query_results(SQL_statement)

    # No data case
    if results in [None, (None, None)]:
        return [], all_query_ids

    # gather the query_ids that didn't get results
    query_ids_with_results = set([query_results[0] for query_results in results])
    no_results_query_ids = [query_id for query_id in all_query_ids if query_id not in query_ids_with_results]
    return results, no_results_query_ids