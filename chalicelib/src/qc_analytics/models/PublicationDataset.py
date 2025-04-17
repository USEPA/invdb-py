from chalicelib.src.qc_analytics.models.Dataset import Dataset
import chalicelib.src.database.methods as db_methods

class PublicationDataset(Dataset): 

    def get_data_array(self) -> list:
        return self.data


    def insert_missing_gwp_column(self) -> None:
        data_array = self.get_data_array()

        if len(data_array) > 0 and "gwp" in data_array[0]: 
            print("gwp is already present in this dataset, skipping insert operation.")
            return 

        ghg_to_gwp_mappings = db_methods.get_ghg_to_gwp_mappings_by_year(self.reporting_year)
        for row in data_array: 
            if row["ghg"] in ghg_to_gwp_mappings:
                row["gwp"] = float(ghg_to_gwp_mappings[row["ghg"]])
            else: 
                print(f"Warning: missing gwp value for chemical {row['ghg']}, assuming gwp of 1.")
                row["gwp"] = 1


    def insert_missing_ghg_category_column(self) -> None:
        data_array = self.get_data_array()

        if len(data_array) > 0 and "ghg_category" in data_array[0]: 
            print("ghg_category is already present in this dataset, skipping insert operation.")
            return 

        ghg_to_ghg_category_mappings = db_methods.get_ghg_to_ghg_category_mappings()
        for row in data_array: 
            if row["ghg"] in ghg_to_ghg_category_mappings:
                row["ghg_category"] = ghg_to_ghg_category_mappings[row["ghg"]]
            else: 
                print(f"Warning: missing ghg_category value for chemical {row['ghg']}, assuming ghg category of {row['ghg']}.")
                row["ghg_category"] = row["ghg"]