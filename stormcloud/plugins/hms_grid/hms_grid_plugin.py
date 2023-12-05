import logging
import os
from shutil import make_archive
from tempfile import TemporaryDirectory

import boto3
from create_hms_grid import write_meta_to_grid
from dotenv import load_dotenv
from common.cloud import split_s3_path

PLUGIN_PARAMS = {"required": ["metadata_s3_uris", "watershed", "output_zip_s3_uri"]}


def create_grid_filename(watershed: str) -> str:
    initials = ""
    for char in watershed:
        if char.isupper():
            initials += char
    return f"{initials}_Transpose.grid"


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

    results_dict = {}

    with TemporaryDirectory() as tmp_dir:
        full_directory_path = os.path.join(tmp_dir, params["directory_name"])
        grid_fn = create_grid_filename(params["watershed"])
        write_meta_to_grid(full_directory_path, grid_fn, params["metadata_s3_uris"], s3_client)
        zip_bucket, zip_key = split_s3_path(params["output_zip_s3_uri"])
        zip_path = os.path.basename(zip_key).replace(".zip", "")
        output_zip = make_archive(zip_path, "zip", tmp_dir, full_directory_path)
        s3_client.upload_file(zip_bucket, zip_key, output_zip)
        results_dict["zip_s3_uri"] = params["output_zip_s3_uri"]

    return results_dict
