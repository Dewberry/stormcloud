""" Script to isolate time periods of storms using meilisearch data and search NOAA s3 zarr data for data matching these time periods to save to DSS format """
import datetime
import enum
import logging
from typing import List, Union

import s3fs
import xarray as xr
from shapely.geometry import MultiPolygon, Polygon

from .cloud import load_watershed


class NOAADataVariable(enum.Enum):
    """Class of potential NOAA data variables to extract zarr data for"""

    APCP = "APCP_surface"
    DLWRF = "DLWRF_surface"
    DSWRF = "DSWRF_surface"
    PRES = "PRES_surface"
    SPFH = "SPFH_2maboveground"
    TMP = "TMP_2maboveground"
    UGRD = "UGRD_10maboveground"
    VGRD = "VGRD_10maboveground"

    @property
    def dss_variable_title(self) -> str:
        if self == NOAADataVariable.APCP:
            return "PRECIPITATION"
        elif self == NOAADataVariable.TMP:
            return "TEMPERATURE"
        else:
            return self.value

    @property
    def measurement_type(self) -> str:
        if self == NOAADataVariable.APCP:
            return "per-cum"
        else:
            return "inst-val"

    @property
    def measurement_unit(self) -> str:
        if self == NOAADataVariable.APCP:
            return "MM"
        elif self == NOAADataVariable.TMP:
            return "K"
        else:
            raise NotImplementedError(f"Unit unknown for data variable {self.__repr__}")


def load_zarr(
    s3_bucket: str,
    s3_key: str,
    access_key_id: str,
    secret_access_key: str,
    data_variables: Union[List[str], None] = None,
) -> xr.Dataset:
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
    logging.info(f"Loading .zarr dataset from s3://{s3_bucket}/{s3_key}")
    s3 = s3fs.S3FileSystem(key=access_key_id, secret=secret_access_key)
    ds = xr.open_zarr(s3fs.S3Map(f"{s3_bucket}/{s3_key}", s3=s3))
    if data_variables:
        logging.info(
            f"Subsetting dataset to data variables of interest: {', '.join(data_variables)}"
        )
        ds = ds[data_variables]
    return ds


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
    geojson_bucket: str,
    geojson_key: str,
    access_key_id: str,
    secret_access_key: str,
) -> xr.Dataset:
    """Extracts zarr data for a specified watershed and transposition region over a specified period

    Args:
        watershed_name (str): Watershed of interest
        domain_name (str): Transposition region
        start_dt (datetime.datetime): Start of period to extract
        end_dt (datetime.datetime): End of period to extract
        data_variables (List[NOAADataVariable]): List of data variables
        zarr_bucket (str): s3 bucket holding zarr data
        geojson_bucket (str): s3 geojson bucket
        geojson_key (str): s3 geojson key
        access_key_id (str): s3 access key ID
        secret_access_key (str): s3 secret access key
    """
    watershed_ds_list = []
    current_dt = start_dt
    watershed_shape = load_watershed(
        geojson_bucket, geojson_key, access_key_id, secret_access_key
    )
    while current_dt < end_dt:
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
            hour_ds.rio.write_crs("epsg:4326", inplace=True)
            watershed_hour_ds = hour_ds.rio.clip(
                [watershed_shape], drop=True, all_touched=True
            )
            watershed_ds_list.append(watershed_hour_ds)
        current_dt += datetime.timedelta(hours=1)
    logging.info("Merging hourly datasets")
    watershed_merged_ds = xr.merge(watershed_ds_list)
    return watershed_merged_ds
