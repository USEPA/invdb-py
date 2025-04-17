from chalicelib.src.qc_analytics.models.Dataset import Dataset
from chalicelib.src.qc_analytics.models.PowerUserDataset import PowerUserDataset
from chalicelib.src.qc_analytics.models.PublicationDataset import PublicationDataset


def dataset_factory(dataset_data: dict or list, qca_event_metadata: dict, output_years: list[int], dataset_role: str) -> Dataset: 
    if "PowerUser" in qca_event_metadata[f"{dataset_role}ObjName"]: 
        return PowerUserDataset(dataset_data, qca_event_metadata, output_years, dataset_role)
    else: # only other type currently is Publication. 
        return PublicationDataset(dataset_data, qca_event_metadata, output_years, dataset_role)