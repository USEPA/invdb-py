from chalicelib.src.AWS.S3.models.S3Session import S3Session
import chalicelib.src.qc_analytics.methods as qca_methods
import chalicelib.src.database.methods as db_methods
import chalicelib.src.database.constants as db_constants
import chalicelib.src.AWS.constants as aws_constants
import chalicelib.src.AWS.methods as aws_methods
import chalicelib.src.general.helpers as helpers
import json


def fetch_datasets_from_s3(
    s3_session: S3Session, qca_event_directory_handle: int = None, is_test: bool = False
) -> tuple[dict, dict, dict, bool]:
    # fetch the metadata object (TODO: replace with an actual S3 JSON metadata file fetch)
    qca_event_metadata = None
    if qca_event_directory_handle is not None:
        helpers.tprint("Downloading metadata file from S3 bucket...")
        metadata_file = s3_session.download_file_from_s3_bucket(
            qca_event_directory_handle + "/metadata.json"
        )
        qca_event_metadata = json.loads(metadata_file)

    # fetch the two datasets' data (TODO: make the two fetches from S3 JSON files occurring in parallel)
    baseline_dataset_data = qca_methods.fetch_dataset_data(
        (
            qca_event_directory_handle + "/baseline.json"
            if not is_test
            else qca_event_metadata["baselineFilename"]
        ),
        s3_session,
        from_local=is_test,
    )
    comparator_dataset_data = qca_methods.fetch_dataset_data(
        (
            qca_event_directory_handle + "/comparator.json"
            if not is_test
            else qca_event_metadata["comparatorFilename"]
        ),
        s3_session,
        from_local=is_test,
    )
    return baseline_dataset_data, comparator_dataset_data, qca_event_metadata


def upload_results_to_s3(
    aggregate_results, raw_data_results, qca_event_directory_handle, s3_session
) -> None:
    s3_session.upload_file_to_s3_bucket(
        json.dumps(aggregate_results),
        qca_event_directory_handle + "/recalculations/aggregate_results.json",
    )
    s3_session.upload_file_to_s3_bucket(
        json.dumps(raw_data_results),
        qca_event_directory_handle + "/recalculations/raw_results.json",
    )


def update_recalc_job_status(status: str, folder_name: str):
    db_methods.perform_query(
        f"""
        UPDATE {db_constants.DB_TABLES['QC_ANALYTICS_VIEWER']}
        SET recalc_job_status = %s
        WHERE folder_name = %s;""",
        (status, folder_name),
    )
