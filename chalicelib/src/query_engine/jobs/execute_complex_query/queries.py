import chalicelib.src.database.constants as db_constants
import chalicelib.src.database.methods as db_methods
from typing import List


# def fetch_dim_report_row(rowid: int):
#     # Prepare your query with placeholders
#     query = f"""SELECT drr.query_formula_id, drr.query_formula_parameters, dr.reporting_year, dr.layer_id
#                 FROM {db_constants.DB_TABLES['DIM_REPORT_ROW']} drr
#                 JOIN {db_constants.DB_TABLES['DIM_REPORT']} dr ON dr.report_id = drr.report_id
#                 JOIN {db_constants.DB_TABLES['DIM_QUERY_FORMULA']} dqf ON drr.query_formula_id = dqf.query_formula_id
#                 WHERE drr.report_row_id = %s
#                     and lower(dqf.formula_prefix) = 'complex';"""
#     return db_methods.get_query_results(query, (rowid,))


def fetch_calc_factor_values(calc_factor_ids: List[int]):
    # Create a string with the correct number of placeholders.
    placeholders = ", ".join(["%s"] * len(calc_factor_ids))
    query = f"""SELECT cf.calc_factor_id, cf.is_constant, cf.value, ci.year, dts.year_id, ci.value 
                FROM {db_constants.DB_TABLES['CALCULATION_FACTOR']} cf
                LEFT JOIN {db_constants.DB_TABLES['CALCULATION_INPUT']} ci
                    on cf.calc_factor_id = ci.calc_factor_id
                LEFT JOIN {db_constants.DB_TABLES['DIM_TIME_SERIES']} dts
                    on ci.year = dts.year
                where cf.calc_factor_id IN ({placeholders});"""
    return db_methods.get_query_results(query, tuple(calc_factor_ids))


def fetch_query_formula_dets(sq_ids: List[int]):
    # Create a string with the correct number of placeholders.
    placeholders = ", ".join(["%s"] * len(sq_ids))
    query = f"""SELECT 'SQ'||sq.simple_query_id, sq.query_formula_id, sq.query_formula_parameters 
                FROM {db_constants.DB_TABLES['DIM_SIMPLE_QUERY']} sq
                where sq.simple_query_id IN ({placeholders});"""
    # Here we unpack the sq_ids list into separate arguments.
    return db_methods.get_query_results(query, tuple(sq_ids))
