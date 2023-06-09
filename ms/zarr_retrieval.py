""" Script to isolate time periods of storms and search NOAA s3 zarr data for data matching these time periods to save to DSS format """
import datetime
import json
import os
from typing import Generator, List, Tuple, Union

import s3fs
import xarray as xr
from client_utils import create_s3_client
from constants import INDEX
from meilisearch import Client
from shapely.geometry import MultiPolygon, Polygon, shape
from storm_query import query_ms

from cluster import write_dss

# from tempfile import TemporaryDirectory
# from uuid import uuid4


def get_time_windows(
    year: int, watershed_name: str, domain_name: str, n: int, declustered: bool, ms_client: Client
) -> Generator[Tuple[datetime.datetime, datetime.datetime], None, None]:
    for hit in query_ms(ms_client, INDEX, watershed_name, domain_name, 0, n, declustered, n, year):
        start_dt_str = hit["start"]["datetime"]
        duration = hit["duration"]
        start_dt = datetime.datetime.fromisoformat(start_dt_str)
        end_dt = start_dt + datetime.timedelta(hours=duration)
        yield start_dt, end_dt


def load_zarr(
    s3_bucket: str,
    s3_key: str,
    access_key_id: str,
    secret_access_key: str,
    data_variables: Union[List[str], None] = None,
) -> xr.Dataset:
    s3 = s3fs.S3FileSystem(key=access_key_id, secret=secret_access_key)
    ds = xr.open_zarr(s3fs.S3Map(f"{s3_bucket}/{s3_key}", s3=s3))
    if data_variables:
        ds = ds[data_variables]
    return ds


def trim_dataset(ds: xr.Dataset, start: datetime.datetime, end: datetime.datetime, mask: Union[Polygon, MultiPolygon]):
    time_selected = ds.sel(time=slice(start, end))
    time_selected.rio.write_crs("epsg:4326", inplace=True)
    time_space_selected = time_selected.rio.clip([mask], drop=True, all_touched=True)
    return time_space_selected


def load_watershed(
    s3_bucket: str, s3_key: str, access_key_id: str, secret_access_key: str
) -> Union[Polygon, MultiPolygon]:
    s3_client = create_s3_client(access_key_id, secret_access_key)
    response = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
    geojson_data = response["Body"].read().decode("utf-8")
    geojson_dict = json.loads(geojson_data)
    features = geojson_dict["features"]
    if len(features) > 1:
        raise ValueError("More than one feature in watershed geojson, only expected one feature")
    geometry_attribute = features[0]["geometry"]
    watershed_geometry = shape(geometry_attribute)
    return watershed_geometry


def write_to_s3_dss(ds: xr.Dataset):
    # with TemporaryDirectory() as temp_dir:
    #     temp_dss_basename = f"{uuid4()}.dss"
    #     temp_dss_path = os.path.join(temp_dir, temp_dss_basename)
    #     write_dss(ds, temp_dss_path)
    write_dss(ds, "./test.dss", "a", "b", "c", "f", 1000)
    exit()


def main(
    watershed_name: str,
    domain_name: str,
    year: int,
    n_storms: int,
    declustered: bool,
    data_variables: List[str],
    zarr_bucket: str,
    zarr_key: str,
    watershed_bucket: str,
    watershed_geojson_key: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
):
    ms_client = create_meilisearch_client(ms_host, ms_api_key)
    zarr_ds = load_zarr(zarr_bucket, zarr_key, access_key_id, secret_access_key, data_variables)
    watershed_geom = load_watershed(watershed_bucket, watershed_geojson_key, access_key_id, secret_access_key)
    for start_dt, end_dt in get_time_windows(year, watershed_name, domain_name, n_storms, declustered, ms_client):
        trimmed = trim_dataset(zarr_ds, start_dt, end_dt, watershed_geom)
        write_to_s3_dss(trimmed)
        exit()


if __name__ == "__main__":
    import argparse

    from client_utils import create_meilisearch_client
    from constants import NOAA_DATA_VARIABLES
    from dotenv import load_dotenv

    load_dotenv()

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    parser = argparse.ArgumentParser()
    parser.add_argument("-w", "--watershed_name", type=str, required=True)
    parser.add_argument("-d", "--domain_name", type=str, required=True)
    parser.add_argument("-y", "--year", type=int, required=True)
    parser.add_argument("-n", "--storm_number", type=int, required=True)
    parser.add_argument("--zarr_bucket", type=str, required=True)
    parser.add_argument("--zarr_key", type=str, required=True)
    parser.add_argument("--watershed_bucket", type=str, required=True)
    parser.add_argument("--watershed_geojson_key", type=str, required=True)
    parser.add_argument("--decluster", type=bool, default=True, required=False)
    parser.add_argument("--data_variables", type=list, default=NOAA_DATA_VARIABLES, required=False)

    args = parser.parse_args()

    main(
        args.watershed_name,
        args.domain_name,
        args.year,
        args.storm_number,
        args.decluster,
        args.data_variables,
        args.zarr_bucket,
        args.zarr_key,
        args.watershed_bucket,
        args.watershed_geojson_key,
        access_key_id,
        secret_access_key,
        ms_host,
        ms_api_key,
    )
