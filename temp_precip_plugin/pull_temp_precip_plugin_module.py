"""
Invokes methods from write_zarr_to_dss with support for process api
"""

import os
from tempfile import TemporaryDirectory
from zipfile import ZipFile

from dotenv import load_dotenv

from ..write_zarr_to_dss import ZarrExtractionInput, generate_dss_from_zarr

PLUGIN_PARAMS = {
    "required": [
        "start_date",
        "end_date",
        "data_variables",
        "watershed_name",
        "zarr_s3_bucket",
        "geojson_s3_path",
        "outzip",
    ],
    "optional": ["write_interval"],
}


def main(params: dict) -> dict:
    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    validated_input = ZarrExtractionInput(
        params["watershed_name"],
        params["start_date"],
        params["end_date"],
        params["data_variables"],
        params["geojson_s3_path"],
        params["zarr_s3_bucket"],
    )
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
    with ZipFile(params["out_zip"], "w") as zf:
        with TemporaryDirectory() as tmp_dir:
            for dss_path, dss_basename in generate_dss_from_zarr(*param_list):
                zf.write(dss_path, dss_basename)
    results_dict = {"output_zip_path": params["out_zip"]}
    return results_dict
