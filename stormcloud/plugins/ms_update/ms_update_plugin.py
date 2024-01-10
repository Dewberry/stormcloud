import json
import logging
import os
from typing import Any, List, Tuple, Union

import boto3
from botocore.exceptions import HTTPClientError
from create_ms_docs import SSTGeom, SSTMeta, SSTS3Document, SSTStart, SSTStats, create_ms_documents, sanitize_for_s3

PLUGIN_PARAMS = {
    "required": [
        "watershed_name",
        "transposition_domain",
        "duration",
        "s3_bucket",
        "tropical_storm_json_s3_uri",
        "ranked_events_json_s3_uri",
    ],
    "optional": [],
}


def main(params: dict) -> str:
    logging.info(f"creating s3 client")
    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    s3_client = session.client("s3")
    tropical_bucket, tropical_key = split_uri(params["tropical_storm_json_s3_uri"])
    # check that JSON for tropical storms exists before beginning processing
    s3_client.head_object(Bucket=tropical_bucket, Key=tropical_key)
    logging.info(
        f"retrieving s3 documents associated with SST run for watershed {params['watershed_name']} and domain {params['transposition_domain']}"
    )
    s3_docs = get_s3_docs(
        s3_client, params["s3_bucket"], params["watershed_name"], params["transposition_domain"], params["duration"]
    )
    logging.info(f"loading tropical storm data from s3 uri {params['tropical_storm_json_s3_uri']}")
    tropical_storms_json = load_json(s3_client, tropical_bucket, tropical_key)
    ms_docs = create_ms_documents(s3_docs, params["s3_bucket"], tropical_storms_json)
    ms_dict_list = [dict(m) for m in ms_docs]
    ranked_bucket, ranked_key = split_uri(params["ranked_events_json_s3_uri"])
    logging.info(f"Uploading ranked documents to s3 at uri {params['ranked_events_json_s3_uri']}")
    json_str = upload_json(s3_client, ranked_bucket, ranked_key, ms_dict_list)
    return json_str


def split_uri(uri: str) -> Tuple[str, str]:
    bucket, *parts = uri.replace("s3://", "").split("/")
    key = "/".join(parts)
    return bucket, key


def load_json(client: Any, bucket: str, key: str) -> dict:
    res = client.get_object(Bucket=bucket, Key=key)
    text = res.get("Body").read().decode()
    data = json.loads(text)
    return data


def upload_json(client: Any, bucket: str, key: str, data: Union[dict, list]) -> str:
    json_str = json.dumps(data)
    client.put_object(Body=json_str, Bucket=bucket, Key=key)
    return json_str


def get_s3_docs(
    client: Any, bucket: str, watershed_name: str, transposition_domain: str, duration: int
) -> List[SSTS3Document]:
    sst_s3_docs = []
    watershed_clean = sanitize_for_s3(watershed_name)
    transposition_domain_clean = sanitize_for_s3(transposition_domain)
    docs_prefix = (
        f"watersheds/{watershed_clean}/{watershed_clean}-transpo-area-{transposition_domain_clean}/{duration}h/docs"
    )
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=docs_prefix):
        contents = page.get("Contents", [])
        for content in contents:
            key = content.get("Key")
            logging.info(f"parsing s3 document with key {key}")
            data = load_json(client, bucket, key)
            sst_doc = SSTS3Document(
                SSTStart(**data["start"]),
                data["duration"],
                SSTStats(**data["stats"]),
                SSTMeta(**data["metadata"]),
                SSTGeom(**data["geom"]),
            )
            sst_s3_docs.append(sst_doc)
    return sst_s3_docs
