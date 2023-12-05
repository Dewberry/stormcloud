import datetime
import logging
import os
from typing import Callable, Tuple

import boto3
import pyproj
from common.cloud import get_last_modification
from common.dss import DSSProductMeta
from common.shared import DSSVariable
from pydsstools.heclib.utils import SHG_WKT
from shapely.geometry import Point


def find_storm(
    bucket: str, watershed: str, transposition_domain: str, start_date: datetime.datetime, s3_client, duration: int
) -> Tuple[str, str]:
    watershed_formatted = watershed.lower().strip().replace(" ", "-")
    transposition_region_formatted = transposition_domain.replace(" ", "")
    dss_key = verify_dss_key(
        bucket, watershed_formatted, transposition_region_formatted, start_date, s3_client, duration
    )
    json_key = verify_json_key(
        bucket, watershed_formatted, transposition_region_formatted, start_date, s3_client, duration
    )
    return dss_key, json_key


# TODO: add function to download hecdss file, open, and retrieve sample pathname to pass to metadata creation
def construct_dss_metadata(
    bucket: str, dss_key: str, json_key: str, s3_client, shg_reproj: Callable, **kwargs
) -> DSSProductMeta:
    result = s3_client.get_object(bucket, json_key)
    json_obj = result["Body"].read().decode()
    watershed = json_obj["metadata"]["watershed_name"]
    watershed_s3_uri = json_obj["metadata"]["watershed_source"]
    transposition_domain = json_obj["metadata"]["transposition_domain_name"]
    transposition_domain_s3_uri = json_obj["metadata"]["transposition_domain_source"]
    start_dt = datetime.datetime.fromisoformat(json_obj["start"]["datetime"])
    end_dt = start_dt + datetime.timedelta(hours=json_obj["duration"])
    data_variables = [DSSVariable.PRECIPITATION]
    point = Point(json_obj["geom"]["center_x"], json_obj["geom"]["center_y"])
    dss_s3_uri = f"s3://{bucket}/{dss_key}"
    last_modification = get_last_modification(s3_client, bucket, dss_key)
    # TODO: Make the limit parameters from query legit
    DSSProductMeta(
        watershed,
        watershed_s3_uri,
        transposition_domain,
        transposition_domain_s3_uri,
        dss_s3_uri,
        start_dt,
        end_dt,
        last_modification,
        data_variables,
        point,
        shg_reproj,
        overall_rank=kwargs.get("overall_rank"),
        rank_within_year=kwargs.get("rank_within_year"),
        year_limit=kwargs.get("top_year_limit"),
        overall_limit=kwargs.get("overall_limit"),
    )
    return DSSProductMeta


def create_metadata_uri(json_key: str, start_date: datetime.datetime) -> str:
    common_prefix = os.path.dirname(json_key)
    metadata_basename = f"{start_date.strftime('%Y%m%d-dss-meta.json')}"
    full_key = os.path.join(common_prefix, metadata_basename)
    return full_key


def verify_dss_key(
    bucket: str,
    watershed: str,
    transposition_region: str,
    start_date: datetime.datetime,
    s3_client,
    duration: int,
) -> str:
    key = f"watersheds/{watershed}/{watershed}-transpo-area-{transposition_region}/{duration}h/dss/{start_date.strftime('%Y%m%d')}.dss"
    try:
        s3_client.head_object(bucket, key)
    except Exception as exc:
        logging.error(f"Error when trying to verify existence of dss key s3://{bucket}/{key}: {exc}")
    return key


def verify_json_key(
    bucket: str,
    watershed: str,
    transposition_region: str,
    start_date: datetime.datetime,
    s3_client,
    duration: int,
) -> str:
    key = f"watersheds/{watershed}/{watershed}-transpo-area-{transposition_region}/{duration}h/docs/{start_date.strftime('%Y%m%d')}.dss"
    try:
        s3_client.head_object(bucket, key)
    except Exception as exc:
        logging.error(f"Error when trying to verify existence of dss key s3://{bucket}/{key}: {exc}")
    return key


def main(
    bucket: str, watershed: str, transposition_domain: str, start_dt: datetime.datetime, s3_client, duration: int
) -> None:
    dss_key, json_key = find_storm(bucket, watershed, transposition_domain, start_dt, s3_client, duration)
    wgs84 = pyproj.CRS("EPSG:4326")
    shg_reproj = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform
    meta = construct_dss_metadata(bucket, dss_key, json_key, s3_client, shg_reproj)
    meta_key = create_metadata_uri(json_key, start_dt)
    s3_client.put_object(Body=meta.as_bytes, Bucket=bucket, Key=meta_key)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "watershed", type=str, help="name of watershed for which storm should be identified; ex: Duwamish"
    )
    parser.add_argument(
        "transposition_domain",
        type=str,
        help="name of transposition domain for which storm should be identified; ex: V01",
    )
    parser.add_argument(
        "start_date", type=str, help="start date of storm of interest in %Y-%m-%d format; ex: 1979-12-30"
    )
    parser.add_argument("bucket", type=str, help="bucket in which sst storm results are kept")
    parser.add_argument(
        "-d",
        "--duration",
        type=int,
        required=False,
        default=72,
        help="duration in hours that the sst models are run; defaults to 72",
    )
    args = parser.parse_args()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    session = boto3.session.Session(access_key_id, secret_access_key)
    s3_client = session.client("s3")
    start_dt = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")

    main(args.bucket, args.watershed, args.transposition_domain, start_dt, s3_client)
