from __future__ import annotations
from chalicelib.src.AWS.models.AWSAuthorizationToken import AWSAuthorizationToken
from chalicelib.src.AWS.S3.models.S3Session import S3Session
import chalicelib.src.AWS.methods as aws_methods
import chalicelib.src.general.globals as invdb_globals
import boto3

def get_global_s3_session() -> S3Session:
    if invdb_globals.APP_S3_SESSION is None: # create the s3 session if it doesn't exist

        # make sure the global aws_auth_token exists and is valid
        if invdb_globals.APP_AWS_AUTH_TOKEN is None or not invdb_globals.APP_AWS_AUTH_TOKEN.is_valid():
            invdb_globals.APP_AWS_AUTH_TOKEN = aws_methods.create_authorization_token()

        # use the global aws_auth_token to create a new s3 session and set it as the global s3 session 
        invdb_globals.APP_S3_SESSION = S3Session(invdb_globals.APP_AWS_AUTH_TOKEN)

    return invdb_globals.APP_S3_SESSION


# used to create the S3 session used as an attribute to the custom S3Session class
def create_s3_session(auth_token: AWSAuthorizationToken) -> boto3.session.Session:
    """Get an AWS session using the authorization token.

    Args:
        auth_token (AWSAuthorizationToken): The authorization token to use to assign the role to the session.

    Returns:
        boto3.session.Session: A session to use in client creation and resource work.
    """
    return boto3.Session(
        aws_access_key_id=auth_token.access_key_id,
        aws_secret_access_key=auth_token.secret_access_key,
        aws_session_token=auth_token.session_token,
    )