""" Script to isolate time periods of storms using meilisearch data and search NOAA s3 zarr data for data matching these time periods to save to DSS format """
import datetime
import enum
import json
import logging
import os
from typing import Generator, List, Tuple, Union

import s3fs
import xarray as xr
from meilisearch import Client
from shapely.geometry import MultiPolygon, Polygon, shape

from .client_utils import create_meilisearch_client, create_s3_client
from .constants import INDEX
from .storm_query import query_ms


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

    def translate_value(self) -> str:
        if self == NOAADataVariable.APCP:
            return "PRECIPITATION"
        elif self == NOAADataVariable.TMP:
            return "TEMPERATURE"
        else:
            return self.value


def get_time_windows(
    year: int, watershed_name: str, domain_name: str, n: int, declustered: bool, ms_client: Client
) -> Generator[Tuple[datetime.datetime, datetime.datetime, int], None, None]:
    """Generates start and end time windows for storms identified in meilisearch database as in the top storms when ranked by mean precipitation

    Args:
        year (int): Year of interest
        watershed_name (str): Watershed of interest
        domain_name (str): Transposition domain of interest
        n (int): Number of top storms from which time windows should be pulled
        declustered (bool): If true, use declustered rank which ensures that the duration of SST models do not overlap when ranking by mean precipitation. If false, use unfiltered rank by mean precipitation.
        ms_client (Client): Client used to query meilisearch database for SST model run information

    Yields:
        Generator[Tuple[datetime.datetime, datetime.datetime, int], None, None]: Yields tuple of start time, end time pulled from storm, and storm mean precipitation rank, respectively
    """
    if declustered:
        search_method_name = "declustered"
    else:
        search_method_name = "true"
    logging.info(
        f"Finding time windows aligning with top {n} storms in year {year} for watershed {watershed_name}, transposition region version {domain_name} using {search_method_name} rank"
    )
    for hit in query_ms(ms_client, INDEX, watershed_name, domain_name, 0, n, declustered, n, year):
        rank = hit["ranks"]["true_rank"]
        if declustered:
            rank = hit["ranks"]["declustered_rank"]
        start_dt_str = hit["start"]["datetime"]
        duration = hit["duration"]
        start_dt = datetime.datetime.fromisoformat(start_dt_str)
        end_dt = start_dt + datetime.timedelta(hours=duration)
        yield start_dt, end_dt, rank


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
        logging.info(f"Subsetting dataset to data variables of interest: {', '.join(data_variables)}")
        ds = ds[data_variables]
    return ds


def trim_dataset(
    ds: xr.Dataset, start: datetime.datetime, end: datetime.datetime, mask: Union[Polygon, MultiPolygon]
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


def load_watershed(
    s3_bucket: str, s3_key: str, access_key_id: str, secret_access_key: str
) -> Union[Polygon, MultiPolygon]:
    """Loads watershed geometry from s3 resource

    Args:
        s3_bucket (str): Bucket holding watershed
        s3_key (str): Key of watershed
        access_key_id (str): AWS access key ID
        secret_access_key (str): AWS secret access key

    Raises:
        err: Value error raised if watershed geojson is not a single feature as expected

    Returns:
        Union[Polygon, MultiPolygon]: shapely geometry pulled from watershed s3 geojson
    """
    logging.info(f"Loading watershed transposition region geometry from geojson s3://{s3_bucket}/{s3_key}")
    s3_client = create_s3_client(access_key_id, secret_access_key)
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    geojson_data = response["Body"].read().decode("utf-8")
    geojson_dict = json.loads(geojson_data)
    features = geojson_dict["features"]
    if len(features) > 1:
        err = ValueError("More than one feature in watershed geojson, only expected one feature")
        logging.exception(err)
        raise err
    geometry_attribute = features[0]["geometry"]
    watershed_geometry = shape(geometry_attribute)
    return watershed_geometry


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
    ms_client = create_meilisearch_client(ms_host, ms_api_key)
    zarr_ds = load_zarr(zarr_bucket, zarr_key, access_key_id, secret_access_key, data_variables)
    watershed_geom = load_watershed(geojson_bucket, geojson_key, access_key_id, secret_access_key)
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


def extract_period_zarr(
    start_dt: datetime.datetime,
    end_dt: datetime.datetime,
    data_variables: List[str],
    zarr_bucket: str,
    geojson_bucket: str,
    geojson_key: str,
    access_key_id: str,
    secret_access_key: str,
) -> xr.Dataset:
    """Extracts zarr data for a specified watershed and transposition region over a specified period and saves them to local data location

    Args:
        watershed_name (str): Watershed of interest
        domain_name (str): Transposition region
        start_dt (datetime.datetime): Start of period to extract
        end_dt (datetime.datetime): End of period to extract
        data_variables (List[str]): List of data variables
        zarr_bucket (str): s3 bucket holding zarr data
        geojson_bucket (str): s3 geojson bucket
        geojson_key (str): s3 geojson key
        access_key_id (str): s3 access key ID
        secret_access_key (str): s3 secret access key
    """
    watershed_ds_list = []
    current_dt = start_dt
    while current_dt < end_dt:
        zarr_key = os.path.join(zarr_bucket, f"{current_dt.year}/{current_dt.strftime('%Y%m%d')}")
        watershed_shape = load_watershed(geojson_bucket, geojson_key, access_key_id, secret_access_key)
        hour_ds = load_zarr(zarr_bucket, zarr_key, access_key_id, secret_access_key, data_variables)
        hour_ds.rio.write_crs("epsg:4326", inplace=True)
        watershed_hour_ds = hour_ds.rio.clip([watershed_shape], drop=True, all_touched=True)
        watershed_ds_list.append(watershed_hour_ds)
        current_dt += datetime.timedelta(hours=1)
    watershed_merged_ds = xr.merge(watershed_ds_list)
    return watershed_merged_ds
