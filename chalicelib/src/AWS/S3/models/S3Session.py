from chalicelib.src.AWS.models.AWSAuthorizationToken import AWSAuthorizationToken
import chalicelib.src.AWS.methods as aws_methods
import chalicelib.src.general.globals as invdb_globals
import boto3
import os


class S3Session:
    def __init__(
        self,
        aws_auth_token: AWSAuthorizationToken
    ):
        self.auth_token = aws_auth_token
        self.session = boto3.Session(
            aws_access_key_id=self.auth_token.access_key_id,
            aws_secret_access_key=self.auth_token.secret_access_key,
            aws_session_token=self.auth_token.session_token,
        )


    def is_valid(self) -> bool:
        """Check if the token is about to expire.
        Returns:
            bool: True if the session's token is about to expire
        """
        return self.auth_token.is_valid()


    def _ensure_session_is_valid(self):
        """will refresh this instance's AWS token and S3 session if it is expired or about to expire"""
        if not self.is_valid():
            self.auth_token = aws_methods.create_authorization_token()
            self.session = create_s3_session(self.auth_token)


    def get_s3_bucket_contents(self, bucket_name: str=None) -> list:
        self._ensure_session_is_valid()

        if bucket_name is None: 
            bucket_name = invdb_globals.S3_BUCKET_NAME
        client = self.session.client("s3", verify=False)
        objects = client.list_objects_v2(Bucket=bucket_name).get("Contents")
        if objects is not None:
            return [{"file": obj.get("Key"), "size": obj.get("Size")} for obj in objects]
        else:
            return []


    def download_file_from_s3_bucket(self, file_name: str, bucket_name: str=None) -> bytes:
        self._ensure_session_is_valid()
        if bucket_name is None: 
            bucket_name = invdb_globals.S3_BUCKET_NAME
        client = self.session.client("s3", verify=False)
        response = client.get_object(Bucket=bucket_name, Key=file_name)
        return response.get("Body").read().decode("utf-8")


    def upload_file_to_s3_bucket(self, file_contents: bytes, file_name: str, bucket_name: str=None) -> None:
        self._ensure_session_is_valid()
        if bucket_name is None: 
            bucket_name = invdb_globals.S3_BUCKET_NAME
        client = self.session.client("s3", verify=False)
        client.put_object(Body=file_contents, Bucket=bucket_name, Key=file_name)