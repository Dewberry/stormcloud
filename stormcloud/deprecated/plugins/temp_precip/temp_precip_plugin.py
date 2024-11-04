"""
Invokes methods from write_zarr_to_dss with support for process api
"""

import datetime
import logging
import os
from tempfile import TemporaryDirectory

import boto3
from common.cloud import create_presigned_url, split_s3_path
from common.shared import convert_noaa_var_to_dss_var
from dotenv import load_dotenv
from write_aorc_zarr_to_dss import SpecifiedInterval, ZarrExtractionInput, generate_dss_from_zarr
from construct_meta import construct_dss_meta
from pydsstools.heclib.utils import SHG_WKT
import pyproj

PLUGIN_PARAMS = {
    "required": [
        "start_date",
        "end_date",
        "data_variables",
        "watershed_name",
        "zarr_s3_bucket",
        "watershed_uri",
        "output_s3_bucket",
        "output_s3_prefix",
    ],
    "optional": [
        "write_interval",
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

    validated_input = ZarrExtractionInput(
        params["watershed_name"],
        params["start_date"],
        params["end_date"],
        params["data_variables"],
        params["watershed_uri"],
        params["zarr_s3_bucket"],
    )

    session = boto3.session.Session(access_key_id, secret_access_key, region_name=aws_region)
    s3_client = session.client("s3")

    result_list = []
    metadata_list = []
    result_dict = {}

    wgs84 = pyproj.CRS("EPSG:4326")
    transform_function = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform

    with TemporaryDirectory() as tmp_dir:
        param_list = [
            tmp_dir,
            validated_input.watershed_name,
            validated_input.start_dt,
            validated_input.end_dt,
            validated_input.noaa_variables,
            validated_input.zarr_s3_bucket,
            validated_input.geojson_bucket,
            validated_input.geojson_key,
            access_key_id,
            secret_access_key,
        ]
        if params.get("write_interval"):
            interval_str = params["write_interval"]
            if interval_str == "year":
                interval = SpecifiedInterval.YEAR
            elif interval_str == "month":
                interval = SpecifiedInterval.MONTH
            elif interval_str == "week":
                interval = SpecifiedInterval.WEEK
            elif interval_str == "day":
                interval = SpecifiedInterval.DAY
            else:
                raise ValueError(
                    f"Unexpected interval value '{interval_str}' given; expected on of ('month', 'week', 'day')"
                )
            param_list.append(interval)
        for dss_path in generate_dss_from_zarr(*param_list):
            # save dss to s3
            dss_basename = os.path.basename(dss_path)
            dss_s3_key = os.path.join(params["output_s3_prefix"], dss_basename)
            logging.info(f"Uploading DSS data to {dss_s3_key}")
            s3_client.upload_file(dss_path, params["output_s3_bucket"], dss_s3_key)
            upload_dt = datetime.datetime.now()
            dss_uri = f"s3://{params['output_s3_bucket']}/{dss_s3_key}"
            result_list.append(dss_uri)

            # construct and save metadata to s3
            dss_vars = [convert_noaa_var_to_dss_var(v).name for v in validated_input.noaa_variables]
            meta = construct_dss_meta(
                validated_input.watershed_name,
                validated_input.geojson_s3_path,
                dss_uri,
                validated_input.start_date,
                validated_input.end_date,
                upload_dt,
                None,
                params.get("storm_x"),
                params.get("storm_y"),
                params.get("overall_rank"),
                params.get("rank_within_year"),
                params.get("overall_limit"),
                params.get("top_year_limit"),
                s3_client,
                dss_vars,
                transform_function,
            )
            s3_client.put_object(Body=meta.as_bytes, Bucket=params["output_s3_bucket"], Key=meta.s3_key)
            metadata_s3_uri = f"s3://{params['output_s3_bucket']}/{meta.s3_key}"
            metadata_list.append(metadata_s3_uri)

    result_dict["results"] = result_list
    ref_links = []
    # Add presigned urls
    for key in result_list:
        bucket, dss_s3_key = split_s3_path(key)
        ref_links.append(
            {
                "href": create_presigned_url(bucket, dss_s3_key),
                "rel": "presigned-url",
                "type": "application/octet-stream",
                "title": dss_s3_key,
            }
        )
    result_dict["links"] = ref_links
    # Add metadata
    result_dict["metadata"] = metadata_list

    return result_dict
