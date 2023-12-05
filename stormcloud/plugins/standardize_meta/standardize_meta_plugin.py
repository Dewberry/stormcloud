import logging
import os

import boto3
import pyproj
from common.cloud import split_s3_path
from construct_meta import construct_dss_meta, guess_dss_uri
from dotenv import load_dotenv
from pydsstools.heclib.utils import SHG_WKT

PLUGIN_PARAMS = {
    "required": ["watershed", "extent_geojson_uri", "start_date"],
    "optional": [
        "duration",
        "storm_x",
        "storm_y",
        "overall_rank",
        "rank_within_year",
        "overall_limit",
        "top_year_limit",
    ],
}


def main(params: dict) -> dict:
    # Eliminate logs for botocore credential finding
    logging.getLogger("botocore").setLevel(logging.WARNING)

    try:
        load_dotenv()
    except:
        logging.warning("No .env file found")

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    aws_region = os.environ["AWS_REGION"]

    session = boto3.session.Session(access_key_id, secret_access_key, region_name=aws_region)
    s3_client = session.client("s3")

    wgs84 = pyproj.CRS("EPSG:4326")
    transform_function = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform
    dss_bucket, _ = split_s3_path(params["extent_geojson_uri"])
    dss_uri = guess_dss_uri(params["extent_geojson_uri"], params["start_date"], params.get("duration", 72))
    meta = construct_dss_meta(
        params["watershed"],
        params["extent_geojson_uri"],
        dss_uri,
        params["start_date"],
        None,
        None,
        params.get("duration", 72),
        params.get("storm_x"),
        params.get("storm_y"),
        params.get("overall_rank"),
        params.get("rank_within_year"),
        params.get("overall_limit"),
        params.get("top_year_limit"),
        s3_client,
        transform_function=transform_function,
    )
    meta_uri = f"s3://{dss_bucket}/{meta.s3_key}"
    logging.info(f"Uploading standardized metadata for {dss_uri} to {meta_uri}")
    logging.debug(f"standardized metadata created: {meta.json}")
    s3_client.put_object(
        Body=meta.as_bytes,
        Bucket=dss_bucket,
        Key=meta.s3_key,
    )
    return {"meta_s3_uri": meta_uri}
