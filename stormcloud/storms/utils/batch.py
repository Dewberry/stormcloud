import os
import json
import boto3


def get_secrets(secret_name: str, region_name: str) -> dict:
    """Load the secrets from the secrets manager"""
    try:
        # this should work in batch
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except:
        # this should work locally
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        client = session.client(service_name="secretsmanager", region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

    return json.loads(get_secret_value_response["SecretString"])
