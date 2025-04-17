import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.qc_analytics.constants as qca_constants
import chalicelib.src.database.methods as db_methods
import chalicelib.src.general.helpers as helpers
from abc import ABC, abstractmethod
import pandas as pd


class Dataset(ABC):

    def __init__(self, data: dict or list, dataset_info: dict, output_years: list[int], dataset_key_prefix: str):
        self.data = data
        self.file_name = dataset_info[f"{dataset_key_prefix}ObjName"]
        self.reporting_year = int(dataset_info[f"{dataset_key_prefix}YearLayerKey"][2:6])
        self.layer_id = 1 if "National" in dataset_info[f"{dataset_key_prefix}YearLayerKey"] else 2
        self.output_years = output_years
        self._data_mode = "landscape"
        self.max_time_series = db_methods.fetch_max_time_series_by_reporting_year(self.reporting_year)


    @abstractmethod
    def get_data_array(self) -> list:
        pass


    def get_emissions_key_column_list(self) -> list:
        '''returns the emissions_key columns present in the dataset in order
        according to the columns in the emissions_key table.'''
        data_array = self.get_data_array()

        if len(data_array) == 0: 
            return []
        
        first_data_row = data_array[0]

        emissions_key_columns_found = []
        for key_column in qc_constants.EMISSIONS_KEY_RAW_COLUMNS:
            if key_column in first_data_row:
                emissions_key_columns_found.append(key_column)

        self.emissions_key_columns_found = emissions_key_columns_found
        return emissions_key_columns_found

    
    def groom_data(self) -> list[str]:
        '''miscellaneous preparation steps to ensure datasets are formatted in a 
           homogenous way to optimize matching. Returns the list of columns for which all 
           rows return a null value if that row belongs to qca_constants.RAW_DATA_KEY_COLUMNS'''
        if self._data_mode != "landscape":
            raise ValueError("You can only groom the data while in landscape state")

        unused_columns = list(qca_constants.RAW_DATA_KEY_COLUMNS) # initialize all columns as unused
        for index, row in enumerate(self.get_data_array()):
            # assert that gwp values are represented as float
            if type(row["gwp"]) != float:
                row["gwp"] = float(row["gwp"])
            # change all empty string values to None 
            for key in row.keys():
                if row[key] in ["", None]: 
                    row[key] = "null"    
            if unused_columns: # remove used columns from the unused list
                for column in unused_columns: 
                    if row[column] != 'null':
                        unused_columns.remove(column)

        return unused_columns


    def clear_unused_columns(self, unused_columns: list[str]) -> None:
        '''sets all row's values to 'null' in columns included in the input list'''
        print("the columns to clear are:", unused_columns)
        if self._data_mode != "landscape":
            raise ValueError("You can only groom the data while in landscape state")
        for row in self.get_data_array():
            row.update({column: 'null' for column in unused_columns})


    def convert_data_to_portrait(self):
        if self._data_mode != "landscape":
            raise ValueError("You can only convert to portrait while in landscape state")
        omitted_columns = [column for column in self.get_data_array()[0].keys() if column not in qc_constants.EMISSIONS_KEY_RAW_COLUMNS]
        self.data = helpers.convert_data_from_landscape_to_portrait(self.get_data_array(), self.max_time_series, omitted_columns=omitted_columns)
        self._data_mode = "portrait"
        print("the portrait data is in the form of", self.data[0])


    def convert_data_to_dataframe(self):
        if self._data_mode != "portrait":
            raise ValueError("You can only convert to portrait while in portrait state")
        
        # construct the dataframe out of the portrait data
        self.data = pd.DataFrame(self.data)
        
        # pad non-numeric year quantities to 0
        all_year_columns = [f'Y{year}' for year in range(qc_constants.EARLIEST_REPORTING_YEAR, self.max_time_series)]
        self.data["weighted_quantity"] = pd.to_numeric(self.data["weighted_quantity"], errors='coerce').fillna(0)
        
        self._data_mode = "dataframe"