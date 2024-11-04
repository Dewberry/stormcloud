import datetime
import logging
import os
from tempfile import TemporaryDirectory
from typing import Callable, List, Union

import pyproj
from common.cloud import get_last_modification, split_s3_path
from common.dss import DSSProductMeta
from pydsstools.heclib.dss import HecDss
from pydsstools.heclib.utils import SHG_WKT
from shapely.geometry import Point
from shapely.ops import transform


def guess_dss_uri(extent_geojson_uri: str, start_date: str, duration: int):
    logging.info(f"Creating dss s3 uri from inputs")
    dss_bucket, geojson_key = split_s3_path(extent_geojson_uri)
    start_dt = datetime.datetime.fromisoformat(start_date)
    watershed_dir = os.path.dirname(geojson_key)
    geojson_basename = os.path.basename(geojson_key)
    geojson_name = geojson_basename[: geojson_basename.rfind(".")]
    dss_key = f"{watershed_dir}/{geojson_name}/{duration}h/dss/{start_dt.strftime('%Y%m%d')}.dss"
    dss_uri = f"s3://{dss_bucket}/{dss_key}"
    logging.debug(f"dss s3 uri identified ({dss_uri})")
    return dss_uri


def construct_dss_meta(
    watershed: str,
    geojson_uri: str,
    dss_uri: str,
    start_date: str,
    end_date: Union[str, None],
    last_modification: Union[datetime.datetime, None],
    duration: Union[int, None],
    storm_x: Union[float, None],
    storm_y: Union[float, None],
    overall_rank: Union[int, None],
    rank_within_year: Union[int, None],
    overall_limit: Union[int, None],
    top_year_limit: Union[int, None],
    s3_client,
    data_variables: str = ["PRECIPITATION"],
    transform_function: Union[Callable, None] = None,
) -> DSSProductMeta:
    if storm_x and storm_y:
        if transform_function == None:
            wgs84 = pyproj.CRS("EPSG:4326")
            transform_function = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform
        ms_point = Point(storm_x, storm_y)
        shg_point = transform(transform_function, ms_point)
        shg_x = shg_point.x
        shg_y = shg_point.y
    else:
        shg_x = None
        shg_y = None
    start_dt = datetime.datetime.fromisoformat(start_date)
    if not end_date and duration != None:
        end_dt = start_dt + datetime.timedelta(hours=duration)
    else:
        end_dt = datetime.datetime.fromisoformat(end_date)
    dss_bucket, dss_key = split_s3_path(dss_uri)
    if not last_modification:
        last_modification = get_last_modification(s3_client, dss_bucket, dss_key)
    sample_pathname_dict = get_sample_pathname_dict(s3_client, dss_bucket, dss_key, data_variables)
    meta = DSSProductMeta(
        watershed,
        geojson_uri,
        dss_uri,
        start_dt,
        end_dt,
        last_modification,
        data_variables,
        sample_pathname_dict,
        shg_x,
        shg_y,
        overall_rank,
        rank_within_year,
        overall_limit,
        top_year_limit,
    )
    return meta


def get_sample_pathname_dict(s3_client, bucket: str, dss_key: str, data_variables: List[str]) -> dict:
    with TemporaryDirectory() as tmp_dir:
        local_fn = os.path.join(tmp_dir, os.path.basename(dss_key))
        s3_client.download_file(bucket, dss_key, local_fn)
        result_dict = {}
        with HecDss.Open(local_fn) as hec_f:
            for data_variable in data_variables:
                sample_dss_pathname = hec_f.getPathnameList(f"/*/*/{data_variable}/*/*/*/", sort=1)[0]
                result_dict[data_variable] = sample_dss_pathname
    return result_dict
