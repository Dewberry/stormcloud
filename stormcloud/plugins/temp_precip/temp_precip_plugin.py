"""
Invokes methods from write_zarr_to_dss with support for process api
"""

import logging
import os
from tempfile import TemporaryDirectory

import boto3
from dotenv import load_dotenv
from write_aorc_zarr_to_dss import (
    SpecifiedInterval,
    ZarrExtractionInput,
    generate_dss_from_zarr,
)
from common.cloud import split_s3_path, create_presigned_url

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
    "optional": ["write_interval"],
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

    session = boto3.session.Session(
        access_key_id, secret_access_key, region_name=aws_region
    )
    s3_client = session.client("s3")

    result_list = []
    result_dict = {}

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
            if interval_str == "month":
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
        for dss_path, dss_basename in generate_dss_from_zarr(*param_list):
            s3_key = os.path.join(params["output_s3_prefix"], dss_basename)
            logging.info(f"Uploading DSS data to {s3_key}")
            s3_client.upload_file(dss_path, params["output_s3_bucket"], s3_key)
            result_list.append(f"s3://{params['output_s3_bucket']}/{s3_key}")

    result_dict["results"] = result_list
    ref_links = []
    # Add presigne urls
    for key in result_list:
        bucket, s3_key = split_s3_path(key)
        ref_links.append(
            {
                "href": create_presigned_url(bucket, s3_key),
                "rel": "presigned-url",
                "type": "application/octet-stream",
                "title": s3_key,
            }
        )
    result_dict["links"] = ref_links

    return result_dict
