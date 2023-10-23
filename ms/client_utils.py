""" Utils script for creating s3 clients and meilisearch clients and handling s3 paths"""
import re
from typing import Tuple

import boto3
from meilisearch import Client


def create_s3_client(access_key_id: str, secret_access_key: str) -> object:
    session = boto3.session.Session(access_key_id, secret_access_key)
    s3_client = session.client("s3")
    return s3_client


def create_meilisearch_client(host: str, api_key: str) -> Client:
    ms_client = Client(host, api_key=api_key)
    return ms_client


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    """Takes an s3 path and splits it into a bucket and key

    Args:
        s3_path (str): s3 path (ex: s3://bucket/key.txt)

    Returns:
        Tuple[str, str]: Tuple with bucket and key (ex: ("bucket", "key.txt"))
    """
    s3_pattern = r"^s3:\/\/([a-zA-Z0-9_\-]+)\/([a-zA-Z0-9_\-\/\.]*)$"
    re_pattern = re.compile(s3_pattern)
    matches = re.search(re_pattern, s3_path)
    bucket = matches.group(1)
    key = matches.group(2)
    return bucket, key
