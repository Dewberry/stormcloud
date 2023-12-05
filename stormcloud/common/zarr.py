""" Script to isolate time periods of storms using meilisearch data and search NOAA s3 zarr data for data matching these time periods to save to DSS format """
import datetime
import logging
from typing import Iterator, List, Tuple, Union
from .shared import NOAADataVariable

import numpy as np
import s3fs
import xarray as xr
from shapely.geometry import MultiPolygon, Polygon
from zarr.errors import GroupNotFoundError


def load_zarr(
    s3_bucket: str,
    s3_key: str,
    access_key_id: str,
    secret_access_key: str,
    data_variables: Union[List[str], None] = None,
) -> Union[xr.Dataset, None]:
    """Load zarr data from s3

    Args:
        s3_bucket (str): s3 bucket from which data should be pulled
        s3_key (str): s3 key for zarr data to pull
        access_key_id (str): Access key ID for AWS credentials
        secret_access_key (str): Secret access key for AWS credentials
        data_variables (Union[List[str], None], optional): List of data variables to pull from zarr data. Defaults to None, meaining data will not be subset.

    Returns:
        xr.Dataset: zarr dataset loaded to xarray dataset
    """
    logging.debug(f"Loading .zarr dataset from s3://{s3_bucket}/{s3_key}")
    try:
        s3 = s3fs.S3FileSystem(key=access_key_id, secret=secret_access_key)
        ds = xr.open_zarr(s3fs.S3Map(f"{s3_bucket}/{s3_key}", s3=s3))
        if data_variables:
            logging.debug(f"Subsetting dataset to data variables of interest: {', '.join(data_variables)}")
            ds = ds[data_variables]
        return ds
    except GroupNotFoundError:
        logging.warning(f"Failed to load .zarr dataset from s3://{s3_bucket}/{s3_key}; key '{s3_key}' may not exist")
        return None


def trim_dataset(
    ds: xr.Dataset,
    start: datetime.datetime,
    end: datetime.datetime,
    mask: Union[Polygon, MultiPolygon],
) -> xr.Dataset:
    """Trims dataset to time window and geometry of interest

    Args:
        ds (xr.Dataset): Dataset to trim
        start (datetime.datetime): Start time of window of interest
        end (datetime.datetime): End time of window of interest
        mask (Union[Polygon, MultiPolygon]): Masking geometry for which data should be pulled

    Returns:
        xr.Dataset: Spatiotemporally trimmed dataset
    """
    logging.info(
        f"Trimming dataset to time window from {start} to {end} and using mask geometry with type {mask.geom_type}"
    )
    time_selected = ds.sel(time=slice(start, end))
    time_selected.rio.write_crs("epsg:4326", inplace=True)
    time_space_selected = time_selected.rio.clip([mask], drop=True, all_touched=True)
    return time_space_selected


def extract_period_zarr(
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    data_variables: List[NOAADataVariable],
    zarr_bucket: str,
    aoi_shape: Union[Polygon, MultiPolygon],
    access_key_id: str,
    secret_access_key: str,
) -> Iterator[Tuple[xr.Dataset, NOAADataVariable]]:
    """Extracts zarr data for a specified area of interest over a specified period

    Args:
        start_dt (datetime.datetime): Start of period to extract
        end_dt (datetime.datetime): End of period to extract
        data_variables (List[NOAADataVariable]): List of data variables
        zarr_bucket (str): s3 bucket holding zarr data
        aoi_shape: (Union[Polygon, MultiPolygon]): Shapely geometry of area of interest for extraction
        access_key_id (str): s3 access key ID
        secret_access_key (str): s3 secret access key
    """
    current_dt = start_dt
    while current_dt < end_dt:
        if current_dt.hour == 0:
            logging.info(f"Loading data for {current_dt}")
        for data_variable in data_variables:
            if data_variable == NOAADataVariable.APCP:
                zarr_key_prefix = "transforms/aorc/precipitation"
            elif data_variable == NOAADataVariable.TMP:
                zarr_key_prefix = "transforms/aorc/temperature"
            else:
                raise ValueError(
                    f"Data variable within provided data variables ({data_variables}) does not have s3 data tracked by Dewberry: {data_variable}"
                )

            zarr_key = f"{zarr_key_prefix}/{current_dt.year}/{current_dt.strftime('%Y%m%d%H')}.zarr"
            hour_ds = load_zarr(zarr_bucket, zarr_key, access_key_id, secret_access_key)
            if hour_ds:
                hour_ds.rio.write_crs("epsg:4326", inplace=True)
                aoi_hour_ds = hour_ds.rio.clip([aoi_shape], drop=True, all_touched=True)
                yield aoi_hour_ds, data_variable
        current_dt += datetime.timedelta(hours=1)


def convert_temperature_dataset(data: xr.Dataset, output_unit: str) -> xr.Dataset:
    data_unit = data[NOAADataVariable.TMP.value].units
    if data_unit != "K":
        raise ValueError(f"Expected temperature data in Kelvin, got measurement unit of {data_unit} instead")
    if output_unit != "K":
        data_shape = data[NOAADataVariable.TMP.value].shape
        c_degrees_difference = np.full(data_shape, 273.15)
        if output_unit == "DEG C":
            data[NOAADataVariable.TMP.value] = np.subtract(data[NOAADataVariable.TMP.value], c_degrees_difference)
        elif output_unit == "DEG F":
            c_data = np.subtract(data[NOAADataVariable.TMP.value], c_degrees_difference)
            scale_difference = np.full(data_shape, 9 / 5)
            scale_data = np.multiply(c_data, scale_difference)
            f_difference = np.full(data_shape, 32)
            f_data = np.add(scale_data, f_difference)
            data[NOAADataVariable.TMP.value] = f_data
        else:
            raise ValueError(
                f"Temperature conversion only supported from Kelvin (K) to Celsius (DEG C) or Farenheit (DEG F); got output unit of {output_unit} instead"
            )
    return data
