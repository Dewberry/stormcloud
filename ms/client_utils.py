""" Utils script for creating s3 clients and meilisearch clients"""
import boto3
from meilisearch import Client


def create_s3_client(access_key_id: str, secret_access_key: str) -> object:
    session = boto3.session.Session(access_key_id, secret_access_key)
    s3_client = session.client("s3")
    return s3_client


def create_meilisearch_client(host: str, api_key: str) -> Client:
    ms_client = Client(host, api_key=api_key)
    return ms_client
