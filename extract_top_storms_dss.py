""" Script to extract NOAA data from .zarr datasets which align to top storms in a specified year from meilisearch data """
import json
import os
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Generator, List, Tuple
from zipfile import ZipFile

from jsonschema import validate

from ms.client_utils import split_s3_path
from ms.zarr_retrieval import NOAADataVariable, extract_zarr_top_storms
from storms.cluster import write_multivariate_dss


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


def validate_input(
    input_json_path: str, schema_path: str = "records/zarr-dss/ms/zarr_input_schema.json"
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
            data_variable.translate_value(): data_variable.value for data_variable in zarr_input.noaa_variables
        }
        outpath_basename = f"{zarr_input.watershed_name.lower().replace(' ', '_')}_rank{rank}_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}.dss"
        outpath = os.path.join(out_dir, outpath_basename)
        logging.info(f"Beginning process of writing to {outpath}")
        write_multivariate_dss(
            ds, data_variable_dict, outpath, "SHG1K", zarr_input.watershed_name.upper(), "AORC", 1000
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
                zarr_input, tmp_dir, access_key_id, secret_access_key, ms_host, ms_api_key
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
    parser.add_argument("output_zip", type=str, help="path to which output DSS file will be zipped and saved")

    args = parser.parse_args()

    botocore_logger = logging.getLogger("botocore")
    botocore_logger.setLevel(level=logging.ERROR)

    main(args.json_path, args.output_zip, access_key_id, secret_access_key, ms_host, ms_api_key)
