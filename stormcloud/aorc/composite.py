from datetime import datetime
from dateutil.relativedelta import relativedelta
import boto3
import re
from tempfile import TemporaryDirectory
from zipfile import ZipFile
from io import BytesIO
from collections import defaultdict
from typing import List, Tuple, Dict
import logging
import xarray as xr
import zarr.storage as storage
import os
from constants import RFC_INFO_LIST

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


class CloudHandler:
    def __init__(self) -> None:
        self.client = self.create_client()

    def create_client(self):
        client = boto3.client(
            service_name="s3",
            aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_DEFAULT_REGION"],
        )
        return client

    def partition_bucket_key_names(self, s3_path: str) -> tuple[str, str]:
        """
        Partition the S3 path into bucket and key. Must start with "s3://"
        """
        if not s3_path.startswith("s3://"):
            raise ValueError(f"s3path does not start with s3://: {s3_path}")
        bucket, _, key = s3_path[5:].partition("/")
        return bucket, key

    def get_object(self, s3_path: str) -> bytes:
        """
        Downloads the object specified by the S3 path and returns its content as bytes.
        """
        bucket, key = self.partition_bucket_key_names(s3_path)
        data = self.client.get_object(Bucket=bucket, Key=key)
        data_bytes = data["Body"].read()
        return data_bytes

    def list_s3_files_with_yearmonth(self, bucket_name: str, prefix: str, yearmonth: str) -> list:
        """
        Retrieves a list of s3 paths from a given bucket and prefix where the file name contains the
        yearmonth str. Ex ("202402")
        """
        s3_paths = []
        paginator = self.client.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        pattern = re.compile(rf".*_{yearmonth}\.zip$")

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if pattern.match(obj["Key"]):
                        s3_paths.append(f"s3://{bucket_name}/{obj['Key']}")
        if len(s3_paths) > 0:
            return s3_paths
        else:
            logging.error(f"No files found at s3://{bucket_name}/{prefix} for yearmonth: {yearmonth}")

    def send_composite_zarr(self, merged_hourly_data: xr.Dataset, output_bucket: str, output_prefix: str) -> None:
        """
        Send the composite Zarr dataset to an S3 bucket using the datasets time as the file name.
        """
        full_time_str = merged_hourly_data.time.dt.strftime("%Y%m%d%H").values[0]
        year_str = merged_hourly_data.time.dt.strftime("%Y").values[0]
        destination_fn = f"s3://{output_bucket}/{output_prefix}/{year_str}/{full_time_str}.zarr"

        store = storage.FSStore(destination_fn)

        # If no chunking is desired for zarr files, un-comment out line below and add encoding=encoding
        # to .to_zarr() parameters. Change variable name if needed.
        # encoding = {"APCP_surface": {"chunks": (1, 3000, 3000)}}
        merged_hourly_data.to_zarr(store, mode="w")
        logging.info(f"Zarr has been sent to {destination_fn}")

    def unzip_file_from_s3(self, s3_path: str, directory: str) -> None:
        """
        Unzip a file from S3 into a given directory.
        """
        data_bytes = self.get_object(s3_path)
        with ZipFile(BytesIO(data_bytes)) as zf:
            zf.extractall(directory)


def generate_yearmonth_list(start_date: str, end_date: str) -> list:
    """
    Constructs a list of strings representing year and month (in 'YYYYMM' format) for
    each month between (and including) the start and end dates.
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    yearmonth_list = []

    current_date = start
    while current_date <= end:
        yearmonth_list.append(current_date.strftime("%Y%m"))
        current_date += relativedelta(months=1)

    return yearmonth_list


def align_files_by_datetime(directory: str) -> Dict[str, List[str]]:
    """
    Searches the given direcotry for all .nc4 files and aligns them by their date and hour.
    The output is a dictionary where the keys are date and hours and the values are all the file paths
    for that date and hour.
    """
    aligned_files = defaultdict(list)

    for root, _, files in os.walk(directory):
        for name in files:
            if name.endswith(".nc4"):
                date_hour = name.split("_")[-1].split(".")[0]
                aligned_files[date_hour].append(os.path.join(root, name))

    sorted_aligned_files = {k: sorted(v) for k, v in sorted(aligned_files.items())}

    return sorted_aligned_files


def create_composite_datset(dataset_paths: List[str]) -> xr.Dataset:
    """
    Opens each NetCDF file, sets its CRS to EPSG:4326 using rioxarray, and merges them into a
    single xarray Dataset.
    """
    if len(dataset_paths) != len(RFC_INFO_LIST):
        logging.error(f"Expected {len(RFC_INFO_LIST)} to match RFC office number, got {len(dataset_paths)}")
    datasets = []
    for dataset_path in dataset_paths:
        ds = xr.open_dataset(dataset_path)
        ds.rio.write_crs(4326, inplace=True)
        datasets.append(ds)
    merged_hourly_data = xr.merge(datasets, compat="no_conflicts", combine_attrs="drop_conflicts")
    return merged_hourly_data


def main(start_date, end_date, bucket_name, zip_files_prefix, output_zarr_prefix):
    cloud_handler = CloudHandler()

    yearmonths = generate_yearmonth_list(start_date, end_date)
    for yearmonth in yearmonths:
        with TemporaryDirectory() as tempdir:
            s3_paths = cloud_handler.list_s3_files_with_yearmonth(bucket_name, zip_files_prefix, yearmonth)
            for s3_path in s3_paths:
                logging.info(f"Unzipping {s3_path}")
                cloud_handler.unzip_file_from_s3(s3_path, tempdir)

            aligned_files = align_files_by_datetime(tempdir)
            for _, matched_files in aligned_files.items():
                merged_data = create_composite_datset(matched_files)
                cloud_handler.send_composite_zarr(merged_data, bucket_name, output_zarr_prefix)


if __name__ == "__main__":
    START_DATE = "2024-03-01"
    END_DATE = "2024-04-01"
    BUCKET_NAME = "tempest"
    ZIP_FILES_PREFIX = "aorc/temp/source"
    OUTPUT_ZARR_PREFIX = "transforms/aorc/temperature"

    main(START_DATE, END_DATE, BUCKET_NAME, ZIP_FILES_PREFIX, OUTPUT_ZARR_PREFIX)
