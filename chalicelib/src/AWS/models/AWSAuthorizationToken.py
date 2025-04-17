import chalicelib.src.AWS.constants as aws_constants
import datetime
import boto3
import json
import os


class AWSAuthorizationToken:
    def __init__(
        self,
        access_key_id: str,
        secret_access_key: str,
        session_token: str,
        expiration: float,
    ):
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.session_token = session_token
        self.expiration = expiration
        self.expiration_date = datetime.datetime.fromtimestamp(expiration)


    def is_valid(self) -> bool:
        """Check if the token is about to expire.

        Returns:
            bool: True if the token is about to expire
        """
        return not (
            self.expiration_date - datetime.timedelta(minutes=aws_constants.EXPIRATION_RENEWAL_THRESHOLD_MINUTES)
        ) <= datetime.datetime.now()