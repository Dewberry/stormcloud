import datetime
import json
import logging
import os
from types import NoneType
from typing import Any, Dict, List, Tuple, Union

import boto3
from create_ranked_docs import SSTGeom, SSTMeta, SSTS3Document, SSTStart, SSTStats, create_ms_documents, sanitize_for_s3

PLUGIN_PARAMS = {
    "required": [
        "watershed_name",
        "transposition_domain",
        "duration",
        "s3_bucket",
    ],
    "optional": [
        "tropical_storm_json_s3_uri",
        "ranked_events_json_s3_uri",
        "start_date",
        "end_date",
    ],
}


def main(params: dict) -> Dict[str, str]:
    logging.info(f"creating s3 client")
    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    s3_client = session.client("s3")
    logging.info(f"getting tropical storm data from s3")
    tropical_storms_json = create_tropical_storms_json(s3_client, params.get("tropical_storm_json_s3_uri"))
    logging.info(
        f"retrieving s3 documents associated with SST run for watershed {params['watershed_name']} and domain {params['transposition_domain']}"
    )
    start_dt = decode_datetime(params.get("start_date"))
    end_dt = decode_datetime(params.get("end_date"))
    s3_docs = get_s3_docs(
        s3_client,
        params["s3_bucket"],
        params["watershed_name"],
        params["transposition_domain"],
        params["duration"],
        start_dt,
        end_dt,
    )
    ranked_docs = create_ms_documents(s3_docs, params["s3_bucket"], tropical_storms_json)
    ranked_dict_list = [dict(m) for m in ranked_docs]
    output_s3_uri = params.get(
        "ranked_events_json_s3_uri",
        create_default_output_uri(
            params["s3_bucket"], params["watershed_name"], params["transposition_domain"], params["duration"]
        ),
    )
    ranked_bucket, ranked_key = split_uri(output_s3_uri)
    logging.info(f"Uploading ranked documents to s3 at uri {output_s3_uri}")
    upload_json(s3_client, ranked_bucket, ranked_key, ranked_dict_list)
    sample_json_str = {"first": json.dumps(ranked_dict_list[0]), "last": json.dumps(ranked_dict_list[-1])}
    return sample_json_str


def decode_datetime(dt_string: Union[str, NoneType]) -> Union[datetime.datetime, NoneType]:
    if dt_string != None:
        return datetime.datetime.strptime(dt_string, "%Y%m%d")
    return None


def create_tropical_storms_json(client: Any, uri: Union[str, NoneType]) -> Union[dict, NoneType]:
    if uri:
        tropical_bucket, tropical_key = split_uri(uri)
        tropical_storms_json = load_json(client, tropical_bucket, tropical_key)
        return tropical_storms_json
    logging.info(f"no s3 uri provided for JSON with tropical storm data, defaulting to None")
    return None


def create_default_output_uri(bucket: str, watershed_name: str, transposition_domain: str, duration: int) -> str:
    logging.info(f"creating default output s3 uri based on bucket, watershed, transposition domain, and duration")
    watershed_clean = sanitize_for_s3(watershed_name)
    transposition_clean = sanitize_for_s3(transposition_domain)
    default_uri = f"s3://{bucket}/watersheds/{watershed_clean}/{watershed_clean}-transpo-area-{transposition_clean}/{duration}h/{watershed_clean}-{transposition_clean}-ranked-events.json"
    return default_uri


def split_uri(uri: str) -> Tuple[str, str]:
    bucket, *parts = uri.replace("s3://", "").split("/")
    key = "/".join(parts)
    return bucket, key


def load_json(client: Any, bucket: str, key: str) -> Union[List[dict], dict]:
    res = client.get_object(Bucket=bucket, Key=key)
    text = res.get("Body").read().decode()
    data: Union[List[dict], dict] = json.loads(text)
    return data


def override_missing_metadata(data: dict, **kwargs) -> dict:
    logging.debug("override missing metadata with kwargs")
    meta: dict = data["metadata"]
    meta_copy = meta.copy()
    for k, v in meta.items():
        if not v and kwargs.get(k) != None:
            logging.warning(
                f"s3 doc had a blank for metadata property {k} - overwriting with keyword argument {kwargs[k]}"
            )
            meta_copy[k] = kwargs[k]
    data["metadata"] = meta_copy
    return data


def upload_json(client: Any, bucket: str, key: str, data: Union[dict, list]) -> str:
    json_str = json.dumps(data)
    client.put_object(Body=json_str, Bucket=bucket, Key=key)
    return json_str


def get_s3_docs(
    client: Any,
    bucket: str,
    watershed_name: str,
    transposition_domain: str,
    duration: int,
    start_dt: Union[datetime.datetime, NoneType],
    end_dt: Union[datetime.datetime, NoneType],
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
            if filter_key(key, duration, start_dt, end_dt):
                logging.info(f"parsing s3 document with key {key}")
                data = load_json(client, bucket, key)
                override_missing_metadata(
                    data, watershed_name=watershed_name.capitalize(), transposition_domain=transposition_domain.lower()
                )
                sst_doc = SSTS3Document(
                    SSTStart(**data["start"]),
                    data["duration"],
                    SSTStats(**data["stats"]),
                    SSTMeta(**data["metadata"]),
                    SSTGeom(**data["geom"]),
                )
                sst_s3_docs.append(sst_doc)
    return sst_s3_docs


def filter_key(
    key: str, duration: int, start_dt: Union[datetime.datetime, NoneType], end_dt: Union[datetime.datetime, NoneType]
) -> bool:
    if start_dt != None and end_dt != None:
        key_basename = os.path.basename(key)
        key_start_dt = datetime.datetime.strptime(key_basename, "%Y%m%d.json")
        key_end_dt = key_start_dt + datetime.timedelta(hours=duration)
        if key_start_dt >= start_dt and key_end_dt <= end_dt:
            logging.debug(f"key passed filter, returning true")
            return True
        logging.debug(f"key failed filter, returning false")
        return False
    logging.debug(f"insufficient datetime filters given to apply filter, returning all as true")
    return True
