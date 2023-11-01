"""
Invokes methods from write_zarr_to_dss with support for process api
"""

import logging
import os
from tempfile import TemporaryDirectory
import boto3

from dotenv import load_dotenv

from write_aorc_zarr_to_dss import ZarrExtractionInput, generate_dss_from_zarr

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
    try:
        load_dotenv()
    except:
        logging.warning("No .env file found")

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    validated_input = ZarrExtractionInput(
        params["watershed_name"],
        params["start_date"],
        params["end_date"],
        params["data_variables"],
        params["watershed_uri"],
        params["zarr_s3_bucket"],
    )

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
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
            param_list.append(params["write_interval"])
        for dss_path, dss_basename in generate_dss_from_zarr(*param_list):
            s3_key = os.path.join(params["output_s3_prefix"], dss_basename)
            s3_client.upload_file(dss_path, params["output_s3_bucket"], s3_key)
            result_list.append(f"s3://{params['output_s3_bucket']}/{s3_key}")

    result_dict["results"] = result_list

    return result_dict
