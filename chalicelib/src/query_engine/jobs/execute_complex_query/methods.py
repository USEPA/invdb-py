from chalicelib.src.query_engine.jobs.execute_complex_query.queries import *
import chalicelib.src.query_engine.methods as qe_methods
import re
from chalicelib.src.query_engine.methods import *
import traceback
import chalicelib.src.general.helpers as helpers

time_series = {}

def execute_complex_query(query_formula_parameters: dict, reporting_year: int, layer_id: int, gwp: str=None):  # , query_formula_parameters: dict):
    global time_series

    # dim_report_info = fetch_dim_report_row(id)
    # if not dim_report_info:
    #     return {"result": f"There is no dim_report_row with ID: {id}"}
    # query_formula_id, query_formula_parameters, reporting_year, layer_id = dim_report_info[0]
    year_obj = qe_methods.get_qe_years_object(reporting_year)
    time_series = year_obj
    print(
        f"Query Formula Parameters: {query_formula_parameters}"
    )
    formula_exists = False
    for key, value in query_formula_parameters.items():
        if key == "formula":
            formula_exists = True
            formula_template = value
            calc_values = get_calculation_values(formula_template, reporting_year, layer_id, gwp)
            if not calc_values:
                raise ValueError(
                    f"Something went wrong when getting calculation values for report_row_id: {id}"
                )
            # Evaluate the expression
            try:
                evaluate_formula(formula_template, calc_values, year_obj)
                print(
                    f"eval(formula) : {year_obj}"
                )  # This will output the result of the formula
                return year_obj
            except Exception as e:
                print(f"Error evaluating: {e}")
                traceback.print_exc()
            break
    if not formula_exists:
        raise ValueError(f"No formula found for complex query for report_row_id {id}")

# Function to dynamically replace placeholders with the function return values
def get_calculation_values(formula, reporting_year, layer_id, gwp: str=None):
    # Regular expression to find text within square brackets
    bracket_pattern = re.compile(r"\[([A-Za-z]+)(\d+)\]")
    matches = bracket_pattern.findall(formula)
    result = {}
    simple_query_ids = []
    calculation_factor_ids = []
    # Loop over all placeholders and replace them with the return value of the corresponding function
    for match in matches:
        # Each 'match' is a tuple where the first item is the sequence of alphabets
        # and the second item is the sequence of numbers following it
        func_name, id = match
        placeholder = f"[{func_name}{id}]"
        id = int(id)
        # Perform different actions based on the prefix of the func_name
        if func_name.lower() == "sq":
            simple_query_ids.append(id)
        elif func_name.lower() == "cf":
            calculation_factor_ids.append(id)
        else:
            raise ValueError(f"No matching function for placeholder {placeholder}")
    sq_values = calculate_sq_values(simple_query_ids, reporting_year, layer_id, gwp)
    if not bool(sq_values) or len(sq_values) != len(simple_query_ids):
        raise Exception(f"There is issue with getting values for Simple Queries: {simple_query_ids}")
    cf_values = calculate_cf_values(calculation_factor_ids)
    result = {**sq_values, **cf_values}
    print(f"result: {result}")
    return result

def calculate_sq_values(sq_ids, reporting_year, layer_id, gwp: str=None):
    if not sq_ids:
        return {}
    # get rid of any duplicate ids
    unique_sq_ids = list(set(sq_ids))
    sq_query_formula_dets = fetch_query_formula_dets(unique_sq_ids)
    print(f"sq_qf: {sq_query_formula_dets}")
    return execute_simple_query(sq_query_formula_dets, reporting_year, layer_id, gwp)
    
def calculate_cf_values(cf_ids):
    if not cf_ids:
        return {}
     # get rid of any duplicate ids
    unique_cf_ids = list(set(cf_ids))
    calc_values = fetch_calc_factor_values(unique_cf_ids)
    if not calc_values:
        raise ValueError(f"Error fetching calculation factors : {cf_ids}")
    # Initialize the result dictionary
    result_dict = {}
    for row in calc_values:
        calc_factor_id, is_constant, cf_value, year, year_id, ci_value = row
        outer_key = 'CF' + str(calc_factor_id)
        if outer_key not in result_dict:
            result_dict[outer_key] = {}
            # construct the entire object. If constant with constant
            # value else assinging 0 so even if some years are missing
            # due to bad data we will have 0 assigned for them
            if is_constant:
                value = cf_value
            else:
                value = 0
            for key in time_series:
                result_dict[outer_key][key] = value
        if not is_constant:
            if year_id:
                result_dict[outer_key][str(year_id)] = ci_value
    return result_dict

def evaluate_formula(formula_template, calc_values, year_obj):
    # Loop through the years in the 'emission' dictionary.
    for year in year_obj.keys():
        # Start with empty calc_val and the original formula template for each calculation
        calc_val = {}
        formula = formula_template
        for key, values in calc_values.items():
            if year in values:
                calc_val[key] = values[year]
            else:
                raise ValueError(f"Value for year {year} is missing for {key}")

        # Replace each key in the formula with its corresponding value from calc_val
        for key, value in calc_val.items():
            formula = formula.replace('['+key+']', str(value))

        # Evaluate the formula after replacement and add the result to the results list
        try:
            # This safely evaluates an expression node or a string containing a Python expression
            year_obj[year] = eval(formula)
        except Exception as e:
            print(f"Error evaluating formula {formula}: {e}")
            year_obj[year] = None


def handle_complex_query_request(queries: list[str], reporting_year: int, layer_id: int, user_id: int):
    '''API endpoint logic that exposes the execute_complex_query() function above. Also supports multiple (single-processing) 
    complex query requests.'''
    response_object = {}
    for index, query_formula in enumerate(queries):
        print("query_formula", query_formula)
        response_object[f"Query {index + 1}"] = execute_complex_query({"formula": query_formula}, reporting_year, layer_id)
    return response_object