import chalicelib.src.database.methods as db_methods
import chalicelib.src.database.constants as db_constants
import chalicelib.src.general.helpers as helpers

def fetch_queries_for_online_report(report_id: int, report_type_id: int) -> tuple:
    # Prepare the query
    if report_type_id == 1: # adds an additional result set for the QC queries if relevant
        query = f"""SELECT drr.report_row_id,
                        dqf.query_formula_id,
                        drr.query_formula_parameters,
                        dr.reporting_year,
                        dr.layer_id,
                        dqt.query_class,
                        dqt.query_type_name,
                        dqt.priority
                    FROM {db_constants.DB_TABLES['DIM_REPORT_ROW']} drr
                        JOIN {db_constants.DB_TABLES['DIM_REPORT']} dr ON dr.report_id = drr.report_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_FORMULA']} dqf ON drr.query_formula_id = dqf.query_formula_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_TYPE']} dqt ON dqt.query_type_id = dqf.query_type_id
                    WHERE dr.report_id = {report_id}"""
    
    if report_type_id == 2: # adds an additional result set for the QC queries if relevant
        query = f"""SELECT dqcrr.qc_report_row_id,
                           dqcrr.emissions_query_formula_id,
                           dqcrr.emissions_query_formula_parameters,
                           dqr.reporting_year,
                           dqr.layer_id,
                           dqt.query_class,
                           dqt.query_type_name,
                           dqt.priority
                    FROM {db_constants.DB_TABLES['DIM_QC_REPORT']} dqr
                        JOIN {db_constants.DB_TABLES['DIM_QC_COMP_REPORT_ROW']} dqcrr ON dqr.qc_report_id = dqcrr.qc_report_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_FORMULA']} dqf ON dqcrr.emissions_query_formula_id = dqf.query_formula_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_TYPE']} dqt ON dqt.query_type_id = dqf.query_type_id
                    WHERE dqr.qc_report_id = {report_id} 
                    UNION ALL
                    SELECT dqcrr.qc_report_row_id,
                           dqcrr.qc_query_formula_id,
                           dqcrr.qc_query_formula_parameters,
                           dqr.reporting_year,
                           dqr.layer_id,
                           dqt.query_class,
                           dqt.query_type_name,
                           dqt.priority
                    FROM {db_constants.DB_TABLES['DIM_QC_REPORT']} dqr
                        JOIN {db_constants.DB_TABLES['DIM_QC_COMP_REPORT_ROW']} dqcrr ON dqr.qc_report_id = dqcrr.qc_report_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_FORMULA']} dqf ON dqcrr.qc_query_formula_id = dqf.query_formula_id
                        JOIN {db_constants.DB_TABLES['DIM_QUERY_TYPE']} dqt ON dqt.query_type_id = dqf.query_type_id
                    WHERE dqr.qc_report_id = {report_id}"""
    
    # execute the query
    results = db_methods.get_query_results(query, (report_id,))

    # return an empty list if no results were found
    if results == (None, None) or results is None or len(results) == 0: 
        helpers.tprint(f"fetch_query_formula_for_report_row(): WARNING: No queries were found with the given Report ID: {report_id} and Report Type ID: {report_type_id}.")
        return []
    
    return results