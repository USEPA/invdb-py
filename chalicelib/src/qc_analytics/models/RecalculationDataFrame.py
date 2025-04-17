from __future__ import annotations
import chalicelib.src.qc_analytics.constants as qca_constants
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.database.methods as db_methods
from typing import Callable
from copy import deepcopy
import pandas as df
import hashlib


class RecalculationDataFrame:

    def __init__(self, data: df.core.frame.DataFrame, index_columns: list[str], reporting_year: int, output_years: list[int]):
        self.output_years = output_years
        self.time_series = [f'Y{year}' for year in range(qc_constants.EARLIEST_REPORTING_YEAR, db_methods.fetch_max_time_series_by_reporting_year(reporting_year) + 1)] 
        self.aggregate_data = self._get_aggregated_data_recursive(data, index_columns, "")
        self.raw_data = self._get_raw_data(data, index_columns)


    def _get_aggregated_data_recursive(self, data, index_columns: list[str], key_string) -> dict:
        layer_data = []
        
        if len(index_columns) > 0: # aggregate layer case (recursive case)
            for key, group in data.groupby(index_columns[0]):
                current_key_string = (key_string + "/" + key).strip('/')
                year_aggregates = {}
                # gather the recalc value aggregates for this group
                for year, total in group.groupby("year")["weighted_quantity"].agg("sum").items():
                    if year in self.output_years:
                        year_aggregates[f"recalc_{year}"] = total
                current_group_info = {
                    "key": current_key_string,
                    "data": {
                        "name": key
                    }
                }
                if index_columns[0] not in qca_constants.FILTER_BY_VALUES: # exclude aggregations on the filter_by layer (usually the top layer)
                    current_group_info["data"].update(year_aggregates)
                
                # recursive step to generate children data for aggregate layers
                current_group_info.update({"children": self._get_aggregated_data_recursive(group, index_columns[1:], current_key_string)})
                # delete empty children lists
                if current_group_info["children"] == []:
                    del current_group_info["children"]
        
                layer_data.append(current_group_info)

        return layer_data


    @staticmethod
    def _find_matching_aggregate_data_object(comparator_recalc_dataframe: list[dict], name: str):
        '''more efficient than using filter()'''
        for data_obj in comparator_recalc_dataframe:
            if data_obj["data"]["name"] == name:
                return data_obj
        return None


    def _get_raw_data(self, data, index_columns: list[str]) -> dict:
        raw_data = []
        keys_with_multiple_rows = 0
        print("input data length: ", len(data))
        # group entire data set by the emissions key columns
        groups = data.groupby(qc_constants.EMISSIONS_KEY_RAW_COLUMNS)
        for key, group in groups: # group by key
            # if there is more than one landscape input row mapping to a key, aggregate their time series values into a single row (still a group in portrait form)
            if len(group) > len(self.time_series): 
                keys_with_multiple_rows += 1
                year_quantities = group[['year', 'weighted_quantity']].groupby("year").sum().reset_index()
            else: 
                year_quantities = group[['year', 'weighted_quantity']]
            
            # construct the aggregate key for this row
            aggregate_key = "/".join(group.iloc[0][index_columns])
            
            # construct the emissions key for this row
            emissions_key_data = group.iloc[0:1][qc_constants.EMISSIONS_KEY_RAW_COLUMNS]
            emissions_key_gwp_col_position = emissions_key_data.columns.get_loc("gwp")
            emissions_key_data = [None if value == "null" else value for value in emissions_key_data.iloc[0].to_list()]
            emissions_key_data[emissions_key_gwp_col_position] = float(emissions_key_data[emissions_key_gwp_col_position]) # convert the gwp column to python float
            emission_key = hashlib.md5(str(tuple(emissions_key_data)).encode(), usedforsecurity=False).hexdigest()
            
            # construct the raw data key for this row
            raw_data_key_data = group.iloc[0:1][qca_constants.RAW_DATA_KEY_COLUMNS]
            raw_data_key_data = [None if value == "null" else value for value in raw_data_key_data.iloc[0].to_list()]
            raw_data_key = hashlib.md5(str(tuple(raw_data_key_data)).encode(), usedforsecurity=False).hexdigest()

            # construct the info object for this row
            current_row_data = {"emissions_key": emission_key, "raw_data_key": raw_data_key} # add the emissions key
            current_row_data.update({column: key[index] for index, column in enumerate(qc_constants.EMISSIONS_KEY_RAW_COLUMNS)}) # add the emissions key columns
            for _, row in year_quantities.iterrows(): # add the time series quantities
                current_row_data[f"Y{int(row['year'])}"] = row["weighted_quantity"]
            
            # insert it this row's info object into the appropriate aggregate child list
            aggregate_group_index = RecalculationDataFrame._search_for_key_within_list(raw_data, aggregate_key)
            if aggregate_group_index is None: # create a new object for the aggregate group
                raw_data.append({"key": aggregate_key, "data": [current_row_data]})
            else: # insert current row into current group's children list
                raw_data[aggregate_group_index]["data"].append(current_row_data)
        
        print("keys_with_multiple_rows", keys_with_multiple_rows)
        return raw_data


    @staticmethod
    def _search_for_key_within_list(search_list: list, key_value: str, key_name:str="key") -> int:
        '''returns the index of the dictionary within a list with the matching key value, otherwise returns None'''
        index = 0
        while index < len(search_list):
            if search_list[index][key_name] == key_value:
                return index
            index += 1

        return None


    @staticmethod
    def _get_operation(parameter: str) -> Callable[float, float]: 
        if parameter in ["mmt", "Difference"]: 
            return lambda x, y: (x - y) 
        if parameter == "percent": 
            return lambda x, y: None if y == 0 else (x - y) / y


    def get_recalculated_aggregate_data(self, comparator_recalc_dataframe: RecalculationDataFrame, parameter: str) -> list[dict]:
        '''performs the recalculation on the input data trees.'''
        operation = RecalculationDataFrame._get_operation(parameter) 
        output_year_keys = [f"recalc_{year}" for year in self.output_years]
        
        return RecalculationDataFrame._get_recalculated_aggregate_data_recursive(self.aggregate_data, comparator_recalc_dataframe.aggregate_data, operation, output_year_keys, level=0)


    @staticmethod
    def _get_recalculated_aggregate_data_recursive(baseline_tree: list[dict], comparator_tree: list[dict], operation: Callable[float, float], output_year_keys: list[str], level: int):
        recalculated_layer = deepcopy(baseline_tree)
        for data_obj in recalculated_layer: 
            comparator_data_obj = RecalculationDataFrame._find_matching_aggregate_data_object(comparator_tree, data_obj["data"]["name"])
            if comparator_data_obj is not None: # if a matching data object is found
                if level != 0: # ignore for top level of aggregation since there are no recalc values
                    data_obj["data"].update({year: operation(data_obj["data"][year], comparator_data_obj["data"][year]) for year in output_year_keys})
                # recurse for children if present in both data sets
                if "children" in data_obj and "children" in comparator_data_obj: 
                    data_obj["children"] = RecalculationDataFrame._get_recalculated_aggregate_data_recursive(data_obj["children"], comparator_data_obj["children"], operation, output_year_keys, level + 1)
                # set all child data to None if comparator data is missing children
                elif "children" in data_obj and "children" not in comparator_data_obj:
                    for child in data_obj["children"]:
                        RecalculationDataFrame._set_all_data_to_none(child, output_year_keys)
            else: # set data object and children to None if no match is found
                RecalculationDataFrame._set_all_data_to_none(data_obj, output_year_keys)

        return recalculated_layer

    
    @staticmethod
    def _set_all_data_to_none(data_obj, output_year_keys: list[str]):
        '''used to set non-matching data and its children to None'''
        data_obj["data"].update({key: None for key in output_year_keys})

        if "children" in data_obj:
            for child in data_obj["children"]:
                RecalculationDataFrame._set_all_data_to_none(child, output_year_keys)    

        
    def get_recalculated_raw_data_obj(self, comparator_raw_data: list, parameter: str) -> list:
        '''perform recalculation between self and another input recalculationDataFrame's raw_data'''
        operation = RecalculationDataFrame._get_operation(parameter) 
        matches = 0
        non_matches = 0
        # create key_maps for input baseline and comparator raw data: 
        input_year_keys = [f"Y{year}" for year in self.output_years]
        comparator_raw_key_map = {}
        for aggregate_group in comparator_raw_data:
            for raw_data_row in aggregate_group["data"]:
                comparator_raw_key_map.update({raw_data_row["raw_data_key"]: {key: raw_data_row[key] for key in input_year_keys}})
        print(f"comparator_raw_key_map ({len(comparator_raw_key_map)} raw data keys): ", list(comparator_raw_key_map.values())[0:2])
        
        recalculated_raw_data = deepcopy(self.raw_data)
        for aggregate_group in recalculated_raw_data:
            for raw_data_row in aggregate_group["data"]:
                raw_data_key = raw_data_row["raw_data_key"]
                try: 
                    matching_comparator_raw_data_row = comparator_raw_key_map[raw_data_key]
                    matches += 1
                    raw_data_row.update({f'recalc_{year}': operation(raw_data_row[f'Y{year}'], matching_comparator_raw_data_row[f'Y{year}']) for year in self.output_years})
                except KeyError: 
                    non_matches += 1
                    raw_data_row.update({f'recalc_{year}': None for year in self.output_years})
                    
        print("raw key matches:", matches)
        print("raw key non-matches:", non_matches)

        return recalculated_raw_data