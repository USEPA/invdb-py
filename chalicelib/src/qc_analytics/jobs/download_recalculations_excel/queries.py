from chalicelib.src.AWS.S3.models.S3Session import S3Session
import chalicelib.src.qc_analytics.methods as qca_methods
import chalicelib.src.general.helpers as helpers
import json


def fetch_qca_object_files_from_s3(
    s3_session: S3Session, qca_event_directory_handle: int
) -> tuple[dict, dict, dict, dict, dict]:
    # fetch the metadata object
    try:
        helpers.tprint("Downloading metadata file from S3 bucket...")
        metadata_file = s3_session.download_file_from_s3_bucket(
            qca_event_directory_handle + "/metadata.json"
        )
        qca_event_metadata = json.loads(metadata_file)
    except: 
        raise ValueError(f"QC analytics object with the given handle {qca_event_directory_handle} could not be retrieved.")

    #fetch the two datasets' data (TODO: make the two fetches from S3 JSON files occurring in parallel)
    try:
        baseline_dataset_data = qca_methods.fetch_dataset_data(
            qca_event_directory_handle + "/baseline.json",
            s3_session
        )    
    except: 
        baseline_dataset_data = None

    try:
        comparator_dataset_data = qca_methods.fetch_dataset_data(
            qca_event_directory_handle + "/comparator.json",
            s3_session
        )
    except: 
        comparator_dataset_data = None

    #fetch the two result datasets' data
    try:
        aggregate_results = qca_methods.fetch_dataset_data(
            qca_event_directory_handle + "/recalculations/aggregate_results.json",
            s3_session
        )    
    except: 
        aggregate_results = None

    try:
        raw_results = qca_methods.fetch_dataset_data(
            qca_event_directory_handle + "/recalculations/raw_results.json",
            s3_session
        )
    except: 
        raw_results = None
    
    return (qca_event_metadata,
           baseline_dataset_data,
           comparator_dataset_data,
           aggregate_results,
           raw_results)