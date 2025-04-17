import chalicelib.src.AWS.S3.methods as s3_methods
import chalicelib.src.qc_analytics.jobs.recalculations_report.queries as recalc_queries
from chalicelib.src.qc_analytics.models.DatasetFactory import dataset_factory
from chalicelib.src.qc_analytics.models.RecalculationDataFrame import RecalculationDataFrame
from chalicelib.src.qc_analytics.models.Dataset import Dataset
import chalicelib.src.general.qc_constants as qc_constants
import chalicelib.src.general.helpers as helpers
import json
import time


def prepare_dataset_for_comparison(dataset) -> None: 
    # get the list of missing emissions key columns for both datasets (sector thru GWP)
    dataset_key_columns = dataset.get_emissions_key_column_list()
    dataset_missing_keys_columns = [column for column in qc_constants.EMISSIONS_KEY_RAW_COLUMNS if column not in dataset_key_columns]
    helpers.tprint("dataset_missing_keys_columns", dataset_missing_keys_columns)
    
    # populate missing key columns as needed using their object's recovery methods
    for missing_column in dataset_missing_keys_columns: 
        column_recovery_method = getattr(dataset, f"insert_missing_{missing_column}_column", None)
        if column_recovery_method and callable(column_recovery_method):
            column_recovery_method()
        else: 
            raise ValueError(f"dataset (type {type(dataset)}) is missing recovery method for column {missing_column}.")

    dataset_key_columns = dataset.get_emissions_key_column_list()
    dataset_missing_keys_columns = [column for column in qc_constants.EMISSIONS_KEY_RAW_COLUMNS if column not in dataset_key_columns]
    helpers.tprint("dataset_missing_keys_columns", dataset_missing_keys_columns)


def convert_dataset_to_recalc_dataframe(dataset: Dataset, qca_event_metadata, output_years, dataset_name: str, test_case_name: str, is_local_test: bool=False) -> RecalculationDataFrame:
    helpers.tprint("constructing baseline recalculation dataframe...")
    start_time = time.perf_counter()  # Start time
    dataset.convert_data_to_portrait()
    dataset.convert_data_to_dataframe()

    recalc_dataframe = RecalculationDataFrame(dataset.data, [qca_event_metadata["ghgOption"]] + qca_event_metadata["columns"], dataset.reporting_year, output_years)
    end_time = time.perf_counter()  # End time
    elapsed_time = end_time - start_time  # Calculate elapsed time

    helpers.tprint(f"The baseline tree took {elapsed_time} seconds to construct.")
    
    if is_local_test:
        with open(f"tests/local/{test_case_name}_{dataset_name}_aggregate.json", 'w') as file: 
            json.dump(recalc_dataframe.aggregate_data, file, indent=4)
        with open(f"tests/local/{test_case_name}_{dataset_name}_raw.json", 'w') as file: 
            json.dump(recalc_dataframe.raw_data, file, indent=4)

    return recalc_dataframe

def get_recalc_dataframes(qca_event_directory_handle, s3_session, test_case_name: str=None, is_local_test: bool=False) -> tuple[RecalculationDataFrame, RecalculationDataFrame]: 
    '''returns the recalc dataframes for the baseline and comparator datasets'''

    baseline_dataset_data, comparator_dataset_data, qca_event_metadata = recalc_queries.fetch_datasets_from_s3(s3_session, qca_event_directory_handle, is_test=bool(test_case_name))

    # change 'sub_category_fuel_1' to 'sub_category_1' and 'fuel1' in metadata's columns value
    print('the old columns list is', qca_event_metadata['columns'])
    sub_category_fuel_1_index = qca_event_metadata['columns'].index('sub_category_fuel_1')
    qca_event_metadata['columns'] = qca_event_metadata['columns'][:sub_category_fuel_1_index] + ['sub_category_1', 'fuel1'] + qca_event_metadata['columns'][sub_category_fuel_1_index + 1:]
    print('the new columns list is', qca_event_metadata['columns'])
    # change ghgOption from ghg or ghgCategory to 'ghg' or 'ghg_category'
    qca_event_metadata['ghgOption'] = 'ghg_category' if 'category' in qca_event_metadata['ghgOption'].lower() else 'ghg'

    # create dataset object instances
    baseline_dataset = dataset_factory(baseline_dataset_data, qca_event_metadata, qca_event_metadata["recalculations"]["outputYears"], dataset_role="baseline")
    comparator_dataset = dataset_factory(comparator_dataset_data, qca_event_metadata, qca_event_metadata["recalculations"]["outputYears"], dataset_role="comparator")

    # make the datasets conform to the same format for optimal comparison
    prepare_dataset_for_comparison(baseline_dataset)
    prepare_dataset_for_comparison(comparator_dataset)

    # additional data preparation steps
    helpers.tprint("grooming baseline...")
    baseline_unused_columns = set(baseline_dataset.groom_data())
    helpers.tprint("grooming comparator...")
    comparator_unused_columns = set(comparator_dataset.groom_data())
    baseline_dataset.clear_unused_columns(list(comparator_unused_columns - baseline_unused_columns))
    comparator_dataset.clear_unused_columns(list(baseline_unused_columns - comparator_unused_columns))

    # build the recalc dataframes
    max_output_year = min(baseline_dataset.max_time_series, comparator_dataset.max_time_series)
    output_years = [year for year in qca_event_metadata["recalculations"]["outputYears"] if year <= max_output_year]
    baseline_recalc_dataframe = convert_dataset_to_recalc_dataframe(baseline_dataset, qca_event_metadata, output_years, "baseline", test_case_name, is_local_test)
    comparator_recalc_dataframe = convert_dataset_to_recalc_dataframe(comparator_dataset, qca_event_metadata, output_years, "comparator", test_case_name, is_local_test)
    del baseline_dataset
    del comparator_dataset

    return baseline_recalc_dataframe, comparator_recalc_dataframe, qca_event_metadata


def handle_recalculations_report_request(qca_event_directory_handle: str, user_id: int, test_case_name: str=None, is_local_test: bool=False):
    # fetch the datasets from S3
    folder_name = qca_event_directory_handle[qca_event_directory_handle.rfind('/') + 1:]
    try:
        s3_session = s3_methods.get_global_s3_session()
        baseline_recalc_dataframe, comparator_recalc_dataframe, qca_event_metadata = get_recalc_dataframes(qca_event_directory_handle, s3_session, test_case_name, is_local_test, )
        
        # perform the recalculation
        helpers.tprint("performing recalculation...")
        start_time = time.perf_counter()  # Start time
        recalculation_aggregate_data = baseline_recalc_dataframe.get_recalculated_aggregate_data(comparator_recalc_dataframe, qca_event_metadata["recalculations"]["parameter"])
        recalculation_raw_data = baseline_recalc_dataframe.get_recalculated_raw_data_obj(comparator_recalc_dataframe.raw_data, qca_event_metadata["recalculations"]["parameter"])
        end_time = time.perf_counter()  # End time
        elapsed_time = end_time - start_time  # Calculate elapsed time
        helpers.tprint(f"The recalculation took {elapsed_time} seconds to perform.")

        # upload results to S3
        helpers.tprint("Uploading results to S3...", end="")
        recalc_queries.upload_results_to_s3(recalculation_aggregate_data, recalculation_raw_data, qca_event_directory_handle, s3_session)
        print(" Done.")

        recalc_queries.update_recalc_job_status("complete", folder_name)
        return {"status": "recalculation job completed successfully"}
    except Exception as e:
        recalc_queries.update_recalc_job_status("failed", folder_name)
        raise e
    
    if is_local_test:
        with open(f"tests/temp/{test_case_name}_output_aggregate.json", "w") as file: 
            json.dump(recalculation_aggregate_data, file, indent=4)
        with open(f"tests/temp/{test_case_name}_output_raw.json", "w") as file: 
            json.dump(recalculation_raw_data, file, indent=4)