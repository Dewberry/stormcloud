from .constants import RFC_INFO_LIST, FTP_HOST
from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
import os
import requests
from io import BytesIO
from typing import List
import urllib3
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
urllib3.disable_warnings()


def create_client():
    client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ["AWS_DEFAULT_REGION"],
    )
    return client


def create_url_list(var: str, first_record: str, last_record: str) -> List[str]:
    """
    Generates a list of URLs for downloading data from the noaa FTP server based on
    the specified variable type (precipitation or temperature), start date, and end date.
    """
    urls = []
    rfcs = [rfc_info.alias for rfc_info in RFC_INFO_LIST]
    start_date = datetime.strptime(first_record, "%Y-%m-%d")
    end_date = datetime.strptime(last_record, "%Y-%m-%d")
    for rfc in rfcs:
        current_datetime = start_date
        while current_datetime <= end_date:
            current_datetime_str = current_datetime.strftime("%Y%m")
            if var == "precipitation":
                precip_url = f"{FTP_HOST}/{rfc}RFC_4km/precipitation/AORC_APCP_4KM_{rfc}RFC_{current_datetime_str}.zip"
                urls.append(precip_url)
            elif var == "temperature":
                temp_url = f"{FTP_HOST}/{rfc}RFC_4km/temperature/AORC_TMPR_4KM_{rfc}RFC_{current_datetime_str}.zip"
                urls.append(temp_url)
            else:
                logging.error("Variable must be either 'precipitation' or 'temperature'")
            current_datetime += relativedelta(months=1)
    return urls


def write_data_to_s3(url: str, s3_bucket: str, prefix: str, s3_client) -> None:
    """
    Downloads the data from the specified URL, stores it in memory, and uploads it to the given S3 bucket.
    """
    try:
        filename = os.path.basename(url)
        year = filename.split("_")[-1][:4]

        response = requests.get(url, verify=False)
        response.raise_for_status()

        file_data = BytesIO(response.content)

        s3_key = f"{prefix}/{year}/{filename}"
        s3_client.upload_fileobj(file_data, s3_bucket, s3_key)

        logging.info(f"Uploaded {filename} to s3://{s3_bucket}/{s3_key}")
    except Exception as e:
        logging.error(f"ERROR UPLOADING {filename} to s3://{s3_bucket}/{s3_key} with error: {e}")


def main(variable: str, first_record: str, last_record: str, bucket: str, prefix: str):
    s3_client = create_client()
    urls = create_url_list(variable, first_record, last_record)

    for url in urls:
        write_data_to_s3(url, bucket, prefix, s3_client)


if __name__ == "__main__":

    VARIABLE = "precipitation"
    FIRST_RECORD = "2022-11-01"
    LAST_RECORD = "2024-04-01"
    S3_BUCKET = "tempest"
    PREFIX = "aorc/precip/source"

    main(VARIABLE, FIRST_RECORD, LAST_RECORD, S3_BUCKET, PREFIX)
