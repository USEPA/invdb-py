import chalicelib.src.general.qc_constants as qc_constants
from fuzzywuzzy import fuzz
from math import floor
import datetime
import hashlib
import json
import re
from copy import copy


def tprint(*args, sep=' ', end='\n', file=None, flush=False):
    RESET = "\033[0m"
    GREEN = "\033[32m"
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    message = sep.join(str(arg) for arg in args)
    print(f"{GREEN}[{timestamp}]{RESET} {message}", end=end, file=file, flush=flush)


def full_class_name(o):
    """returns the class name as well as the package it came from"""
    class_name = o.__class__
    module = class_name.__module__
    if module == "builtins":
        return class_name.__qualname__
    return module + "." + class_name.__qualname__


def plurality_agreement(singular_form, plural_form, quantity):
    """e.g. return the string 'file' v.s. 'files' when quantity = 1"""
    return singular_form if quantity == 1 else plural_form


def simplify_whitespace(str: str) -> str or None:
    """Replaces all whitespace instances with a single space. Also Removes leading and trailing whitespace.

    Args:
    str (string): input string to be simplified.
    """
    if str is None: 
        return None
    return re.sub("[ \t\n]+", " ", str).strip()


def remove_duplicates_from_list(input_list: list) -> list:
    """Removes duplicate elements from the list while preserving the original order"""
    seen = set()
    unique_list = []
    for item in input_list:
        if item not in seen:
            seen.add(item)
            unique_list.append(item)
    return unique_list


def rindex(input_list: list, value) -> int:
    """Returns the index of the last occurence of a value in a list. If not found, -1 is returned.
       If the passed value is a list or tuple, function will return the last index of any value matching any of value's elements."""
    if type(value) in (tuple, list):
        for i in range(len(input_list) - 1, -1, -1):
            if input_list[i] in value:
                return i
    else:
        for i in range(len(input_list) - 1, -1, -1):
            if input_list[i] == value:
                return i

    return -1


def get_sql_list_str(list: list) -> str:
    if len(list) == 0:
        tprint("WARNING: an empty SQL list literal has been generated.")
        return "()"

    sql_list_str = "("
    for item in list:
        if isinstance(item, str) and "::" not in item and item.upper() != "NULL":
            sql_list_str += f"'{item}'"
        elif item is None or (isinstance(item, str) and item.upper() == "NULL"):
            sql_list_str += "NULL"
        else:
            sql_list_str += str(item)
        sql_list_str += ", "

    return sql_list_str[:-2] + ")"

def get_sql_array_str(list: list) -> str:
    return "ARRAY[" + get_sql_list_str(list)[1:-1] + "]"


def is_numeric_string(s):
    try:
        float(s)  # or int(s) for integer values
        return True
    except ValueError:
        return False


def get_timestamp(format="%Y-%m-%d %H:%M:%S"):
    """returns the current time as a string"""
    return datetime.datetime.now().strftime(format)


def list_object_attribute_values(object):
    """prints all attribute values of an object"""
    all_attributes = dir(object)
    custom_attributes = [attr for attr in all_attributes if not attr.startswith("__")]
    for attr in custom_attributes:
        tprint(f"{attr}: {getattr(object, attr)}")


def parse_report_formula_arguments(input_string: str) -> ([str], bool): # useful for parsing report formulas
    """returns a list of single quote-wrapped substrings within the input string as"""
    substrings = []
    start_index = 0

    left_over_string = input_string
    while True:
        start_quote = input_string.find("'", start_index)
        if start_quote == -1:
            break

        end_quote = input_string.find("'", start_quote + 1)
        if end_quote == -1:
            break

        substring = input_string[start_quote + 1:end_quote]
        left_over_string = left_over_string[:start_quote] + " "*(len(substring)+2) + left_over_string[end_quote+1:]
        substrings.append(substring)

        start_index = end_quote + 1

    # reduce the left_over_string to just within the parentheses
    left_over_string = left_over_string[left_over_string.find('(') + 1:left_over_string.find(')')].strip()

    has_unquoted_params = not all([character in [' ', ','] for character in left_over_string])
    return substrings, has_unquoted_params


def soft_compare_strings(string1: str, string2: str) -> bool:
    """will return if the two strings are at least 90% similar based on the Levenshtein Distance Algorithm"""
    similarity_ratio = fuzz.ratio(string1, string2)
    similarity_percentage = (similarity_ratio / 100) * 100
    if similarity_percentage >= 90:
        return True
    else:
        return False


def is_valid_json(json_string: str) -> bool:
    try:
        json_object = json.loads(json_string)
    except json.JSONDecodeError:
        return False
    return True


def tuples_to_dict(tuples: [tuple]) -> dict:
    """ takes a list of tuples and creates a dictionary where the unique first element values are the keys, mapped to a list of corresponding second element values
        e.g. input: [('A', 1), ('B', 2), ('A', 3), ('C', 4), ('B', 5)]
            outputs: {'A': [1, 3], 'B': [2, 5], 'C': [4]}
    """
    dict_obj = {key: list(set([value for _, value in tuples if _ == key])) for key, _ in tuples}
    if all([len(value) == 1 for value in dict_obj.values()]):
        return {key: value[0] for key, value in dict_obj.items()}
    else: 
        print("some values had a different number of results:")
        print([f"{key}: {value}" for key, value in dict_obj.items() if len(value) != 1])
        return dict_obj


def generate_data_object_key(data: dict, key_field_names: [str] = ["data_key"], non_key_field_names: [str] =  None) -> str:
    return hashlib.md5(
        str(
            ([data[field] for field in key_field_names]) if non_key_field_names is None else ([data[field] for field in data.keys() if field not in non_key_field_names])
        ).encode(),
        usedforsecurity=False,
    ).hexdigest()

## keep these methods together ^^ and vv

def order_landscape_json_columns(data: [dict], time_series: [int]):
    """takes a JSON array of homogeneous elements and reorders the dictionary attributes in each row to hold all the key values in the front and the ordered quantity values at the end"""
    if not isinstance(data, list):
        raise TypeError(f"helpers.order_landscape_json_columns: Error: input must be a JSON-valid array.")
    if len(data) == 0:
        return []
    if "1990" not in data[0] and "Y1990" not in data[0] and "y1990" not in data[0]:
        raise ValueError(f"helpers.order_landscape_json_columns: Error: input must be in landscape format.")
        # reorder the columns so that the quantity values are in time-series order at the end of each object row
    
    if "1990" in data[0]:
        column_order = [key_column_key for key_column_key in data[0].keys() if isinstance(key_column_key, str) and not key_column_key.isnumeric()]
        return [{**{column: (None if column not in row else row[column]) for column in column_order}, **{f"Y{year}": row[year] for year in time_series}} for row in data]
    else: # if "Y1990" in data[0]
        year_prefix = 'Y'
        if "y1990" in data[0]:
            year_prefix = 'y'
        column_order = [key_column_key for key_column_key in data[0].keys() if isinstance(key_column_key, str) and not key_column_key[1:].isnumeric()]
        column_order += [f"{year_prefix}{year}" for year in time_series]
        return [{column: (None if column not in row else row[column]) for column in column_order} for row in data]

## keep these methods together ^^ and vv

def order_portrait_json_columns(data: [dict]):
    """takes a JSON array of homogeneous elements and reorders the dictionary attributes in each row to hold all the key values in the front and the ordered quantity value at the end"""
    tprint("Reordering the columns without a transpose.")
    if not isinstance(data, list):
        raise TypeError(f"helpers.order_portrait_json_columns: Error: input must be a JSON-valid array.")
    if len(data) == 0:
        return []
    if "year" not in data[0] or "weighted_quantity" not in data[0]:
        raise ValueError(f"helpers.order_portrait_json_columns: Error: input json array elements are missing expected attributes 'year' and/or 'weighted_quantity'.")
        # reorder the columns so that the quantity values are in time-series order at the end of each object row
    
    column_order = []
    # put econ_sector and subsector at the front of the column order if present
    if "econ_sector" in data[0].keys(): 
        column_order.append("econ_sector") 
    if "Economic Sector" in data[0].keys(): 
        column_order.append("Economic Sector") 
    if "econ_subsector" in data[0].keys():
        column_order.append("econ_subsector")
    if "Economic SubSector" in data[0].keys(): 
        column_order.append("Economic SubSector")

    # add the remaining columns to the column order except put year and weighted quantity
    column_order += [key for key in data[0].keys() if key not in ("year", "weighted_quantity")] + ["year", "weighted_quantity"]
    # redefine the data to strictly use the column order
    column_sorted_data = [{col: (None if col not in row else row[col]) for col in column_order} for row in data] # sort the columns
    del data
    column_sorted_data.sort(key=lambda row: [str(None if col not in row else row[col]) for col in column_order]) # now sort the rows, using the column list as the key
    return column_sorted_data
    
## keep these methods together ^^ and vv

def transpose_json_to_landscape(data: [dict], time_series: [int]) -> [dict]:
    """returns a new version of the input data structure that is in landscape format"""
    # input checks
    if not isinstance(data, list):
        raise TypeError(f"helpers.transpose_json_to_landscape: Error: input must be a JSON-valid array.")
    if len(data) == 0:
        return []
    if "1990" in data[0] or "Y1990" in data[0] or "y1990" in data[0]:
        print("helpers.transpose_json_to_landscape: Info: input is already in landscape format. Skipping transpose operation, reordering columns only.") 
        # reorder the columns so that the quantity values are in time-series order at the end of each object row
        return order_landscape_json_columns(data, time_series)
    if "year" not in data[0] or "weighted_quantity" not in data[0]:
        raise ValueError("helpers.transpose_json_to_landscape: Error: input json array elements are missing expected attributes 'year' and/or 'weighted_quantity'.")

    # processing: transpose the data from portrait to landscape
    transposed_data = {} # relates key to {emissions_key_data + time series of quantities}
    for row in data:
        current_data_key = generate_data_object_key(row, non_key_field_names=["year", "weighted_quantity"])
        if current_data_key not in transposed_data: 
            row.update({"years": {row["year"]: row["weighted_quantity"]}}) # temp attribute to isolate the year quantity attributes for ordering later
            del row["year"]
            del row["weighted_quantity"]
            transposed_data.update({current_data_key: row})
        else:
            transposed_data[current_data_key]["years"].update({row["year"]: row["weighted_quantity"]})
    
    # order the attributes in each row
    transposed_data = [{**key_rows, **{f"Y{year}": (None if year not in key_rows["years"] else key_rows["years"][year]) for year in time_series}} for key_rows in transposed_data.values()] # order the quantities
    
    for row in transposed_data: # get rid of the temporary years attribute
        del row["years"]

    return transposed_data

## keep these methods together ^^ and vv

def transpose_json_to_portrait(data: [dict], time_series: [int]) -> [dict]:
    """returns a new version of the input data structure that is in portrait format"""
    tprint("Transposing a JSON to portrait...")
    # input checks
    if not isinstance(data, list):
        raise TypeError(f"helpers.transpose_json_to_portrait: Error: input must be a JSON-valid array.")
    if len(data) == 0:
        return []
    if "year" in data[0]:
        print("helpers.transpose_json_to_portrait: Info: input is already in portrait format. Skipping transpose operation, reordering columns only.") 
        # reorder the columns so that the quantity values are in time-series order at the end of each object row
        return order_portrait_json_columns(data)
    if 1900 not in data[0] and "Y1990" not in data[0] and "y1990" not in data[0]:
        raise ValueError("helpers.transpose_json_to_portrait: Error: input json array elements are missing expected attributes '1990' or 'Y1990'.")

    # processing: transpose the data from portrait to landscape
    prefixed_years = "Y1990" in data[0] 
    lowercase_prefixed_years = "y1990" in data[0]
    
    if prefixed_years:
        year_prefix = 'Y'
    elif lowercase_prefixed_years:
        year_prefix = 'y'
    else: 
        year_prefix = ''

    key_columns = [key_column_key for key_column_key in data[0].keys() if isinstance(key_column_key, str) and not (key_column_key[1:].isnumeric() if prefixed_years or lowercase_prefixed_years else key_column_key.isnumeric())]
    transposed_data = [] 
    for row in data:
        for year in time_series: # create on object of refined data per row per year
            transposed_data.append({**{key: (None if key not in row else row[key]) for key in key_columns}, **{"year": year, "weighted_quantity": (None if f"{year_prefix}{year}" not in row else row[f"{year_prefix}{year}"]) if prefixed_years else row[year]}}) # order the data columns consistently
    
    return order_portrait_json_columns(transposed_data)


def convert_data_from_landscape_to_portrait(data: list[dict], max_time_series: int, custom_time_series: list[int]=None, omitted_columns=None) -> list[dict]:
    '''assumes data is a list of landscape datapoints, with year columns formatted as "Y1990", "Y1991", ...''' 
    if len(data) == 0:
        return []

    time_series = custom_time_series if custom_time_series is not None else range(qc_constants.EARLIEST_REPORTING_YEAR, max_time_series + 1)
    year_columns = [f"Y{year_value}" for year_value in time_series]
    column_names = [key for key in data[0].keys() if key not in year_columns + omitted_columns]

    portrait_data = []
    for index, row in enumerate(data):
        non_year_column_data = {column: row[column] for column in column_names}
        for year in year_columns:
            current_portrait_row = copy(non_year_column_data)
            try: 
                current_portrait_row.update({"year": int(year[1:]), "weighted_quantity": row[year]})
            except KeyError:
                current_portrait_row.update({"year": int(year[1:]), "weighted_quantity": None})
            portrait_data.append(current_portrait_row)

    return portrait_data