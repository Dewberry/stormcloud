import json
import logging
import re
from typing import Tuple, Union

import boto3
from shapely.geometry import MultiPolygon, Polygon, shape


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


def load_watershed(
    s3_bucket: str, s3_key: str, access_key_id: str, secret_access_key: str
) -> Union[Polygon, MultiPolygon]:
    """Loads watershed geometry from s3 resource

    Args:
        s3_bucket (str): Bucket holding watershed
        s3_key (str): Key of watershed
        access_key_id (str): AWS access key ID
        secret_access_key (str): AWS secret access key

    Raises:
        err: Value error raised if watershed geojson is not a single feature as expected

    Returns:
        Union[Polygon, MultiPolygon]: shapely geometry pulled from watershed s3 geojson
    """
    logging.info(f"Loading watershed transposition region geometry from geojson s3://{s3_bucket}/{s3_key}")
    session = boto3.session.Session(access_key_id, secret_access_key)
    s3_client = session.client("s3")
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    geojson_data = response["Body"].read().decode("utf-8")
    geojson_dict = json.loads(geojson_data)
    features = geojson_dict["features"]
    if len(features) > 1:
        err = ValueError("More than one feature in watershed geojson, only expected one feature")
        logging.exception(err)
        raise err
    geometry_attribute = features[0]["geometry"]
    watershed_geometry = shape(geometry_attribute)
    return watershed_geometry
