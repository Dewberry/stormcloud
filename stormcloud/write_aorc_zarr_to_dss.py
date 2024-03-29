""" Script to generate DSS files from zarr data for either temperature or precipitation for a specified watershed and time period """

import datetime
import enum
import json
import logging
import os
from dataclasses import dataclass, field
from tempfile import TemporaryDirectory
from typing import Iterator, List
from zipfile import ZipFile

from common.cloud import load_aoi, split_s3_path
from common.dss import DSSWriter
from common.zarr import NOAADataVariable, convert_temperature_dataset, extract_period_zarr
from jsonschema import validate


class SpecifiedInterval(enum.Enum):
    DAY = enum.auto()
    WEEK = enum.auto()
    MONTH = enum.auto()
    YEAR = enum.auto()


@dataclass
class ZarrExtractionInput:
    watershed_name: str
    start_date: str
    end_date: str
    data_variables: List[str]
    geojson_s3_path: str
    zarr_s3_bucket: str
    start_dt: datetime.datetime = field(init=False)
    end_dt: datetime.datetime = field(init=False)
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
        self.geojson_bucket, self.geojson_key = split_s3_path(self.geojson_s3_path)
        self.noaa_variables = self.__transform_data_variable_list()
        self.start_dt = datetime.datetime.strptime(self.start_date, "%Y-%m-%d")
        self.end_dt = datetime.datetime.strptime(self.end_date, "%Y-%m-%d")


def generate_dss_from_zarr(
    output_dir: str,
    aoi_name: str,
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    data_variables: List[NOAADataVariable],
    zarr_bucket: str,
    geojson_bucket: str,
    geojson_key: str,
    access_key_id: str,
    secret_access_key: str,
    write_interval: SpecifiedInterval = SpecifiedInterval.MONTH,
    output_resolution_km: int = 1,
) -> Iterator[str]:
    """Generates dss dataset for a given period using appropriate zarr data, trimmed to a given aoi and written out at a specified interval

    Args:
        output_dir (str): directory to which dss output is saved
        aoi_name (str): name of AOI to use in pathnames in dss file creation
        start_dt (datetime.datetime): start of period of interest to use when searching for zarr data
        end_dt (datetime.datetime): end of period of interest to use when searching for zarr data
        data_variables (List[NOAADataVariable]): list of data variables available from AORC data
        zarr_bucket (str): bucket containing zarr data
        geojson_bucket (str): bucket containing aoi geojson
        geojson_key (str): key of aoi geojson
        access_key_id (str): AWS access key id
        secret_access_key (str): AWS secret access key
        write_interval (SpecifiedInterval, optional): interval at which records are flushed to a complete file. Defaults to SpecifiedInterval.MONTH.
        output_resolution_km (int): kilometer resolution to use when writing out dss files

    Yields:
        Iterator[Tuple[str, str]]: yields the filepath of the dss file created
    """
    aoi_shape = load_aoi(geojson_bucket, geojson_key, access_key_id, secret_access_key)
    current_dt = start_dt
    while current_dt < end_dt:
        if write_interval == SpecifiedInterval.YEAR:
            current_dt_next = current_dt.replace(year=current_dt.year + 1)
        elif write_interval == SpecifiedInterval.MONTH:
            current_dt_next = current_dt.replace(month=current_dt.month + 1)
        elif write_interval == SpecifiedInterval.WEEK:
            current_dt_next = current_dt + datetime.timedelta(days=7)
        else:
            current_dt_next = current_dt + datetime.timedelta(days=1)
        if current_dt_next > end_dt:
            current_dt_next = end_dt
        outpath_basename = f"{aoi_name.lower().replace(' ', '_')}_{current_dt.strftime('%Y%m%d')}_{current_dt_next.strftime('%Y%m%d')}.dss"
        logging.debug(f"Current: {current_dt}; Next: {current_dt_next}; End: {end_dt}")
        outpath = os.path.join(output_dir, outpath_basename)
        shg_res_name = f"SHG{output_resolution_km}K"
        m_res = 1000 * output_resolution_km
        with DSSWriter(outpath, shg_res_name, aoi_name.upper(), "AORC", m_res) as writer:
            for hour_ds, data_variable in extract_period_zarr(
                current_dt,
                current_dt_next,
                data_variables,
                zarr_bucket,
                aoi_shape,
                access_key_id,
                secret_access_key,
            ):
                if data_variable == NOAADataVariable.TMP:
                    hour_ds = convert_temperature_dataset(hour_ds, data_variable.measurement_unit)
                labeled_data_variable = (
                    data_variable.dss_variable_title,
                    {
                        "variable": data_variable.value,
                        "measurement": data_variable.measurement_type,
                        "unit": data_variable.measurement_unit,
                    },
                )
                writer.write_data(hour_ds, labeled_data_variable)
        current_dt = current_dt_next
        if writer.records > 0:
            yield writer.filepath


def validate_input(
    input_json_path: str,
    schema_path: str = "records/zarr-dss/multivariate/zarr_input_schema.json",
) -> ZarrExtractionInput:
    """Validates JSON document using schema

    Args:
        input_json_path (str): Path to JSON document containing inputs used in .zarr extraction process
        schema_path (str, optional): Path to JSON schema. Defaults to "records/zarr/multivariate/zarr_input_schema.json"

    Returns:
        ZarrInput: Validated input data
    """
    with open(input_json_path, "r") as input_f:
        input_data = json.load(input_f)
    with open(schema_path, "r") as schema_f:
        schema_data = json.load(schema_f)
    validate(input_data, schema=schema_data)
    return ZarrExtractionInput(**input_data)


def main(
    input_json_path: str,
    out_zip: str,
    access_key_id: str,
    secret_access_key: str,
    write_interval: SpecifiedInterval,
) -> None:
    """Validates input, generates dss files, and zips all to a zip file

    Args:
        input_json_path (str): JSON file with input parameters
        out_zip (str): output zipfile path
        access_key_id (str): AWS access key id
        secret_access_key (str): AWS secret access key
        write_interval (SpecifiedInterval): interval at which dss records are flushed to separate files
    """
    validated_input = validate_input(input_json_path)
    with ZipFile(out_zip, "w") as zf:
        with TemporaryDirectory() as tmp_dir:
            for dss_path in generate_dss_from_zarr(
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
                write_interval,
            ):
                dss_basename = os.path.basename(dss_path)
                zf.write(dss_path, dss_basename)


if __name__ == "__main__":
    import argparse
    import logging

    from dotenv import load_dotenv

    logging.getLogger("botocore").setLevel(logging.WARNING)

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "json_path",
        type=str,
        help="JSON path for DSS extraction; should follow format of records/zarr/multivariate/zarr_input_schema.json",
    )
    parser.add_argument(
        "output_zip",
        type=str,
        help="path to which output DSS file will be zipped and saved",
    )
    parser.add_argument(
        "--write_interval",
        type=str,
        choices=["year", "month", "week", "day"],
        default="month",
        help="Interval at which DSS files will be written out. Defaults to month.",
    )

    args = parser.parse_args()

    if args.write_interval == "year":
        interval = SpecifiedInterval.YEAR
    elif args.write_interval == "month":
        interval = SpecifiedInterval.MONTH
    elif args.write_interval == "week":
        interval = SpecifiedInterval.WEEK
    else:
        interval = SpecifiedInterval.DAY

    main(args.json_path, args.output_zip, access_key_id, secret_access_key, interval)
