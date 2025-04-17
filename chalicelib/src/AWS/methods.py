from chalicelib.src.AWS.models.AWSAuthorizationToken import AWSAuthorizationToken
import chalicelib.src.general.globals as invdb_globals
import chalicelib.src.AWS.constants as aws_constants
import requests
import json
import os


def create_authorization_token(client_role: str=None) -> AWSAuthorizationToken or None:
    if client_role in (None, aws_constants.APP_TASK_ROLE_NAME):
        role_key = os.getenv("INVDB_APP_TASK_ROLE")
    elif client_role == aws_constants.DATA_ANALYTICS_ROLE_NAME:
        role_key = os.getenv("INVDB_DATA_ANALYTICS_ROLE")
    
    response = requests.get(
        invdb_globals.AWS_AUTH_PERMISSIONS_SERVICE,
        headers={"x-api-key": role_key},
        verify=False
    )
    print(response)
    if response.ok:
        response_json = response.json()
        return AWSAuthorizationToken(
            response_json.get("AccessKeyId"),
            response_json.get("SecretAccessKey"),
            response_json.get("SessionToken"),
            response_json.get("Expiration"),
        )
    return None