from chalicelib.src.AWS.S3.models.S3Session import S3Session
import chalicelib.src.AWS.S3.methods as s3_methods
import json

def fetch_dataset_data(dataset_handle: str, s3_session: S3Session, from_local: bool=False) -> dict:
    '''retrieves the file with the specified handle from s3, or from a local file path if from_local is True.'''
    data = {}
    if from_local:
        with open(dataset_handle, 'r') as data_file: 
            data = json.load(data_file)
    else: 
        data = json.loads(s3_session.download_file_from_s3_bucket(dataset_handle))

    return data
