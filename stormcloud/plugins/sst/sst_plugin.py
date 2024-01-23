"""
Invokes methods from extract_storms_v2 with support for process api
"""

import os

from extract_storms_v2 import main as interior_main, get_client_session, RunSetting
from common.cloud import create_presigned_url, split_s3_path

PLUGIN_PARAMS = {
    "required": [
        "start_date",
        "hours_duration",
        "watershed_name",
        "watershed_uri",
        "transposition_domain",
        "domain_uri",
        "s3_bucket",
        "s3_prefix",
    ],
    "optional": [
        "atlas14_uri",
        "scale_max",
    ],
}


def create_s3_uri(bucket: str, key: str) -> str:
    return f"s3://{os.path.join(bucket, key)}"


def main(params: dict) -> dict:
    run_setting = RunSetting.LOCAL
    session, s3_client = get_client_session(run_setting)
    input_params = params.copy()
    input_params["session"] = session
    input_params["domain_name"] = input_params["transposition_domain"]
    del input_params["transposition_domain"]
    del input_params["s3_bucket"]
    del input_params["s3_prefix"]
    # Run SST
    png_path, dss_path, doc_path = interior_main(**input_params)

    # Upload png, dss, and documentation to s3
    png_key = os.path.join(params["s3_prefix"], "pngs", os.path.basename(png_path))
    s3_client.upload_file(png_path, params["s3_bucket"], png_key)
    dss_key = os.path.join(params["s3_prefix"], "dss", os.path.basename(dss_path))
    s3_client.upload_file(dss_path, params["s3_bucket"], dss_key)
    doc_key = os.path.join(params["s3_prefix"], "docs", os.path.basename(doc_path))
    s3_client.upload_file(doc_path, params["s3_bucket"], doc_key)

    png_s3_uri = create_s3_uri(params["s3_bucket"], png_key)
    dss_s3_uri = create_s3_uri(params["s3_bucket"], dss_key)
    doc_s3_uri = create_s3_uri(params["s3_bucket"], doc_key)

    results_dict = {"png": png_s3_uri, "dss": dss_s3_uri, "metadata": doc_s3_uri}
    ref_links = []

    # Add presigned urls
    for key, value in results_dict.items():
        bucket, s3_key = split_s3_path(value)
        exp_url = create_presigned_url(bucket, s3_key)
        ref_links.append(
            {
                "href": exp_url,
                "rel": "presigned-url",
                "type": "application/octet-stream",
                "title": key,
            }
        )

    results_dict["links"] = ref_links

    return results_dict
