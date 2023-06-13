""" Script to isolate time periods of storms and search NOAA s3 zarr data for data matching these time periods to save to DSS format """
import enum
import datetime
import json
import logging
from typing import Generator, List, Tuple, Union

import s3fs
import xarray as xr
from meilisearch import Client
from shapely.geometry import MultiPolygon, Polygon, shape

from .client_utils import create_meilisearch_client, create_s3_client
from .constants import INDEX
from .storm_query import query_ms


class NOAADataVariable(enum.Enum):
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


def extract_zarr_for_watershed_storms(
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
