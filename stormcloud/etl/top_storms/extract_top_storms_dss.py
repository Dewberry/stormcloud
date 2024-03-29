""" Script to extract NOAA data from .zarr datasets which align to top storms in a specified year from meilisearch data """
import datetime
import json
import os
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Generator, List, Tuple
from zipfile import ZipFile

import xarray as xr
from jsonschema import validate
from meilisearch import Client

from common.cloud import split_s3_path, load_aoi
from common.zarr import NOAADataVariable, load_zarr, trim_dataset
from common.dss import write_dss
from ms.identify import get_time_windows


@dataclass
class ZarrMeilisearchInput:
    """Class which cleans validated JSON input for pulling storm data from 1-km resolution NOAA .zarr data
    stored on s3 according to year and top storm filters applied from meilisearch ranked ata

    Raises:
        ValueError: Error raised if data variable supplied is not of an expected type
    """

    watershed_name: str
    domain_name: str
    year: int
    n_storms: int
    declustered: bool
    data_variables: List[str]
    zarr_s3_path: str
    geojson_s3_path: str
    zarr_bucket: str = field(init=False)
    zarr_key: str = field(init=False)
    geojson_bucket: str = field(init=False)
    geojson_key: str = field(init=False)
    noaa_variables: List[NOAADataVariable] = field(init=False)

    @staticmethod
    def __transform_data_variable(original_data_variable: str) -> NOAADataVariable:
        """Ensures that all data variables supplied align with enum class of expected data variables

        Args:
            original_data_variable (str): Data variable supplied

        Raises:
            ValueError: Error if data variable is not member of NOAADataVariable enum class

        Returns:
            NOAADataVariable: Matched variable
        """
        for noaa_data_variable in NOAADataVariable:
            if original_data_variable == noaa_data_variable.value:
                return noaa_data_variable
        name_list = [e.value for e in NOAADataVariable]
        raise ValueError(
            f"{original_data_variable} not a member of enum NOAADataVariable; expecting one of following: {', '.join(name_list)}"
        )

    def __transform_data_variable_list(self) -> List[NOAADataVariable]:
        """Applies data variable transformation to list of data variables

        Returns:
            List[NOAADataVariable]: List of transformed data variables
        """
        noaa_variable_list = []
        for data_variable in self.data_variables:
            noaa_variable = self.__transform_data_variable(data_variable)
            noaa_variable_list.append(noaa_variable)
        return noaa_variable_list

    def __post_init__(self):
        """Secondary processing on input fields"""
        self.zarr_bucket, self.zarr_key = split_s3_path(self.zarr_s3_path)
        self.geojson_bucket, self.geojson_key = split_s3_path(self.geojson_s3_path)
        self.noaa_variables = self.__transform_data_variable_list()


def extract_zarr_top_storms(
    watershed_name: str,
    domain_name: str,
    year: int,
    n_storms: int,
    declustered: bool,
    data_variables: List[str],
    zarr_bucket: str,
    zarr_key: str,
    geojson_bucket: str,
    geojson_key: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
) -> Generator[Tuple[xr.Dataset, datetime.datetime, datetime.datetime, int], None, None]:
    """Extracts zarr data from for a specified watershed in a specified year with a specified transposition region, coordinating which storms to select using data on meilisearch

    Args:
        watershed_name (str): Watershed name
        domain_name (str): Transposition domain name
        year (int): Year of interest
        n_storms (int): Number of storms to pull for year, watershed, and transposition region, selecting n top storms ranked by mean precipitation
        declustered (bool): If True, use declustered ranking for ranking storms, meaning storms within the same duration of modeling will not appear together in ranking. If False, no filter is used when ranking by mean precipitation.
        data_variables (List[str]): Variables to which zarr dataset will be subset
        zarr_bucket (str): Bucket holding zarr data to pull
        zarr_key (str): Key of zarr data to pull
        geojson_bucket (str): Bucket of watershed data
        geojson_key (str): Key of watershed geojson
        access_key_id (str): AWS access key ID
        secret_access_key (str): AWS secret access key
        ms_host (str): Meilisearch host
        ms_api_key (str): Meilisearch API key used to access meilisearch

    Yields:
        Generator[Tuple[xr.Dataset, datetime.datetime, datetime.datetime, int], None, None]: Yields a tuple containing a trimmed zarr dataset, the start time of the window of interest for that storm, the end time of the window of interest for that storm, and storm rank
    """
    logging.info("Starting extraction process")
    ms_client = Client(ms_host, ms_api_key)
    zarr_ds = load_zarr(zarr_bucket, zarr_key, access_key_id, secret_access_key, data_variables)
    watershed_geom = load_aoi(geojson_bucket, geojson_key, access_key_id, secret_access_key)
    for start_dt, end_dt, rank in get_time_windows(year, watershed_name, domain_name, n_storms, declustered, ms_client):
        t_diff = end_dt - start_dt
        hours_diff = int(t_diff.total_seconds() / 60 / 60)
        trimmed = trim_dataset(zarr_ds, start_dt, end_dt, watershed_geom)
        trimmed_time_length = len(trimmed["time"])
        if trimmed_time_length != hours_diff:
            logging.warning(
                f"Time duration in hours does not match trimmed dataset length. Expected {hours_diff} records, got {trimmed_time_length}"
            )
            hours_to_eliminate = trimmed_time_length - hours_diff
            logging.info(f"Eliminating first {hours_to_eliminate} record(s)")
            clean_ds = trimmed.isel(time=slice(hours_to_eliminate, trimmed_time_length))
            logging.info(f"Now has {len(clean_ds['time'])} records")
        else:
            clean_ds = trimmed
        yield clean_ds, start_dt, end_dt, rank


def validate_input(
    input_json_path: str,
    schema_path: str = "records/zarr-dss/ms/zarr_input_schema.json",
) -> ZarrMeilisearchInput:
    """Validates JSON document using schema

    Args:
        input_json_path (str): Path to JSON document containing inputs used in .zarr extraction process
        schema_path (str, optional): Path to JSON schema. Defaults to "records/zarr/ms/zarr_input_schema.json".

    Returns:
        ZarrInput: Validated input data
    """
    with open(input_json_path, "r") as input_f:
        input_data = json.load(input_f)
    with open(schema_path, "r") as schema_f:
        schema_data = json.load(schema_f)
    validate(input_data, schema=schema_data)
    return ZarrMeilisearchInput(**input_data)


def extract_and_write_zarr_ms(
    zarr_input: ZarrMeilisearchInput,
    out_dir: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
) -> Generator[Tuple[str, str], None, None]:
    """Extracts storms determined by input parameters from NOAA .zarr data to DSS files

    Args:
        zarr_input (ZarrInput): Validated input
        out_dir (str): Directory to which data is saved
        access_key_id (str): Access key ID for s3
        secret_access_key (str): Secret access key for s3
        ms_host (str): URL of meilisearch database host for db holding ranked storms
        ms_api_key (str): API key of meilisearch database for db holding ranked storms

    Yields:
        Generator[Tuple[str, str], None, None]: Generates full paths to DSS files written and their associated basenames
    """
    for ds, start_dt, end_dt, rank in extract_zarr_top_storms(
        zarr_input.watershed_name,
        zarr_input.domain_name,
        zarr_input.year,
        zarr_input.n_storms,
        zarr_input.declustered,
        zarr_input.data_variables,
        zarr_input.zarr_bucket,
        zarr_input.zarr_key,
        zarr_input.geojson_bucket,
        zarr_input.geojson_key,
        access_key_id,
        secret_access_key,
        ms_host,
        ms_api_key,
    ):
        data_variable_dict = {
            data_variable.dss_variable_title: {
                "variable": data_variable.value,
                "measurement": data_variable.measurement_type,
                "unit": data_variable.measurement_unit,
            }
            for data_variable in zarr_input.noaa_variables
        }
        outpath_basename = f"{zarr_input.watershed_name.lower().replace(' ', '_')}_rank{rank}_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.dss"
        outpath = os.path.join(out_dir, outpath_basename)
        logging.info(f"Beginning process of writing to {outpath}")
        write_dss(
            ds,
            data_variable_dict,
            outpath,
            "SHG1K",
            zarr_input.watershed_name.upper(),
            "AORC",
            1000,
        )
        yield outpath, outpath_basename


def main(
    input_json_path: str,
    out_zip: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
    **kwargs,
) -> str:
    """Extracts storms determined by input parameters from NOAA .zarr data to DSS files, then zips those files

    Args:
        input_json_path (str): JSON file path to validate
        out_zip (str): Zip file to which compressed data is written
        access_key_id (str): Access key ID for s3
        secret_access_key (str): Secret access key for s3
        ms_host (str): URL of meilisearch database host for db holding ranked storms
        ms_api_key (str): API key of meilisearch database for db holding ranked storms

    Returns:
        str: Path of zip file
    """
    logging.info("Validating JSON input document")
    zarr_input = validate_input(input_json_path, **kwargs)
    with ZipFile(out_zip, "w") as zf:
        with TemporaryDirectory() as tmp_dir:
            for dss_path, dss_basename in extract_and_write_zarr_ms(
                zarr_input,
                tmp_dir,
                access_key_id,
                secret_access_key,
                ms_host,
                ms_api_key,
            ):
                zf.write(dss_path, dss_basename)
    logging.info(f"Zipped all DSS docs to {out_zip}")
    return out_zip


if __name__ == "__main__":
    import argparse
    import logging

    from dotenv import load_dotenv

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "json_path",
        type=str,
        help="JSON path for DSS extraction; should follow format of records/zarr/ms/zarr_input_schema.json",
    )
    parser.add_argument(
        "output_zip",
        type=str,
        help="path to which output DSS file will be zipped and saved",
    )

    args = parser.parse_args()

    botocore_logger = logging.getLogger("botocore")
    botocore_logger.setLevel(level=logging.ERROR)

    main(
        args.json_path,
        args.output_zip,
        access_key_id,
        secret_access_key,
        ms_host,
        ms_api_key,
    )
