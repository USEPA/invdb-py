from chalicelib.src.qc_analytics.models.Dataset import Dataset

class PowerUserDataset(Dataset): 

    def get_data_array(self) -> list:
        return self.data["Data by UNFCCC-IPCC Sectors"]


    def insert_missing_geo_ref_column(self):
        data_array = self.get_data_array()

        if len(data_array) > 0 and "geo_ref" in data_array[0]: 
            print("geo_ref is already present in this dataset, skipping insert operation.")
            return 

        for row in data_array: 
            row["geo_ref"] = None