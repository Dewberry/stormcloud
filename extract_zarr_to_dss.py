""" Script to extract NOAA data from .zarr datasets which align to top storms in a specified year """
import json
import os
import re
from dataclasses import dataclass, field
from typing import List, Tuple

from jsonschema import validate

from storms.cluster import write_multivariate_dss
from ms.zarr_retrieval import NOAADataVariable, extract_zarr_for_watershed_storms


@dataclass
class ZarrInput:
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
        for noaa_data_variable in NOAADataVariable:
            if original_data_variable == noaa_data_variable.value:
                return noaa_data_variable
        name_list = [e.value for e in NOAADataVariable]
        raise ValueError(
            f"{original_data_variable} not a member of enum NOAADataVariable; expecting one of following: {', '.join(name_list)}"
        )

    def __transform_data_variable_list(self) -> List[NOAADataVariable]:
        noaa_variable_list = []
        for data_variable in self.data_variables:
            noaa_variable = self.__transform_data_variable(data_variable)
            noaa_variable_list.append(noaa_variable)
        return noaa_variable_list

    def __post_init__(self):
        self.zarr_bucket, self.zarr_key = split_s3_path(self.zarr_s3_path)
        self.geojson_bucket, self.geojson_key = split_s3_path(self.geojson_s3_path)
        self.noaa_variables = self.__transform_data_variable_list()


def validate_input(input_json_path: str, schema_path: str = "records/zarr/zarr_input_schema.json") -> ZarrInput:
    with open(input_json_path, "r") as input_f:
        input_data = json.load(input_f)
    with open(schema_path, "r") as schema_f:
        schema_data = json.load(schema_f)
    validate(input_data, schema=schema_data)
    return ZarrInput(**input_data)


def split_s3_path(s3_path: str) -> Tuple[str, str]:
    s3_pattern = r"^s3:\/\/([a-zA-Z0-9_\-]+)\/([a-zA-Z0-9_\-\/\.]*)$"
    re_pattern = re.compile(s3_pattern)
    matches = re.search(re_pattern, s3_path)
    bucket = matches.group(1)
    key = matches.group(2)
    return bucket, key


def main(
    input_json_path: str,
    out_dir: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
    **kwargs,
):
    zarr_input = validate_input(input_json_path, **kwargs)
    for ds, start_dt, end_dt, rank in extract_zarr_for_watershed_storms(
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
        outpath_basename = f"{zarr_input.watershed_name.lower().replace(' ', '_')}_rank{rank}_{start_dt.strftime('%Y%m%d')}_{end_dt.strftime('%Y%m%d')}"
        outpath = os.path.join(out_dir, outpath_basename)
        logging.info(f"Beginning process of writing to {outpath}")
        write_multivariate_dss(
            ds, data_variable_dict, outpath, "SHG1K", zarr_input.watershed_name.upper(), "AORC", 1000
        )


if __name__ == "__main__":
    import logging
    import sys

    from dotenv import load_dotenv

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    input_json_path, out_dir = sys.argv[1], sys.argv[2]

    botocore_logger = logging.getLogger("botocore")
    botocore_logger.setLevel(level=logging.ERROR)

    main(input_json_path, out_dir, access_key_id, secret_access_key, ms_host, ms_api_key)
