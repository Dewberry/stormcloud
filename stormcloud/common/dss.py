from dataclasses import dataclass
import datetime
import json
import logging
import os
import sys
import warnings
from typing import Dict, List, Tuple, Union

import numpy as np
import pyproj
from affine import Affine
from pydsstools.heclib.dss.HecDss import Open
from pydsstools.heclib.utils import SHG_WKT, dss_logging, gridInfo, lower_left_xy_from_transform
from shapely.geometry import Point
from shapely.ops import transform

from .shared import decode_data_variable, MSStormResult

warnings.filterwarnings("ignore")

import rioxarray as rxr
import xarray as xr

# quietly set dss_logging to level ERROR
# without setting the root logger to ERROR will write warning when changing
logging.root.setLevel(logging.ERROR)
dss_logging.config(level="Error")
logging.root.setLevel(logging.INFO)


class DSSProductMeta:
    def __init__(
        self,
        model_extent_name: str,
        model_extent_geojson_s3_uri: str,
        dss_s3_uri: str,
        start_dt: datetime.datetime,
        end_dt: datetime.datetime,
        last_modification: datetime.datetime,
        data_variables: List[str],
        sample_pathnames: Dict[str, str],
        shg_x: Union[float, None] = None,
        shg_y: Union[float, None] = None,
        overall_rank: Union[int, None] = None,
        rank_within_year: Union[int, None] = None,
        overall_limit: Union[int, None] = None,
        top_year_limit: Union[int, None] = None,
        s3_output_prefix: Union[str, None] = None,
    ) -> None:
        self.model_extent_name = model_extent_name
        self.model_extent_geojson_s3_uri = model_extent_geojson_s3_uri
        self.dss_s3_uri = dss_s3_uri
        self.start_dt = start_dt
        self.end_dt = end_dt
        self.last_modification = last_modification
        self.sample_pathnames = sample_pathnames
        self.overall_rank = overall_rank
        self.rank_within_year = rank_within_year
        self.top_year_limit = top_year_limit
        self.overall_limit = overall_limit
        self.shg_x, self.shg_y = shg_x, shg_y
        self.s3_output_prefix = s3_output_prefix
        self.data_variables = [decode_data_variable(v) for v in data_variables]
        self.qualified_extent_name = self.__get_qualified_extent_name()

    def __get_qualified_extent_name(self) -> str:
        geojson_basename = os.path.basename(self.model_extent_geojson_s3_uri)
        geojson_name = geojson_basename[: geojson_basename.rfind(".")]
        return geojson_name

    @property
    def json(self) -> dict:
        json_output = {
            "model_extent_name": self.model_extent_name,
            "model_extent_geojson_s3_uri": self.model_extent_geojson_s3_uri,
            "dss_s3_uri": self.dss_s3_uri,
            "start_date": self.start_dt.isoformat(),
            "end_date": self.end_dt.isoformat(),
            "last_modification": self.last_modification.isoformat(),
            "sample_pathnames": self.sample_pathnames,
            "shg_x": self.shg_x,
            "shg_y": self.shg_y,
            "overall_rank": self.overall_rank,
            "rank_within_year": self.rank_within_year,
            "overall_limit": self.overall_limit,
            "top_year_limit": self.top_year_limit,
            "data_variables": [v.name for v in self.data_variables],
        }
        return json_output

    @property
    def as_bytes(self) -> bytes:
        json_bytes = json.dumps(self.json).encode("utf-8")
        return json_bytes

    @property
    def s3_key(self):
        prefix = self.s3_output_prefix
        if not prefix:
            prefix = f"watersheds/{self.model_extent_name.lower()}/{self.qualified_extent_name}/standardized_metadata"
        key_basename = f"{self.start_dt.strftime('%Y%m%d')}_{self.end_dt.strftime('%Y%m%d')}_SST_metadata.json"
        key = os.path.join(prefix, key_basename)
        return key


class DSSWriter:
    def __init__(
        self,
        dss_path: str,
        path_a: str,
        path_b: str,
        path_f: str,
        resolution: int,
        verbose: bool = False,
        grid_type: str = "shg-time",
        cell_zero_xcoord: int = 0,
        cell_zero_ycoord: int = 0,
    ):
        self.filepath = dss_path
        self.a = path_a
        self.b = path_b
        self.f = path_f
        self.resolution = resolution
        self.print_pydss = verbose

        self.grid_type = grid_type
        self.cell_zero_xcoord = cell_zero_xcoord
        self.cell_zero_ycoord = cell_zero_ycoord
        self.dss_file = None
        self.records = 0

    def __enter__(self):
        if not self.print_pydss:
            # Suppress printing from pydsstools
            sys.stdout = open(os.devnull, "w")  # prevent prints to stdout
        self.open()
        return self

    def __exit__(self, *args):
        exceptions = [arg for arg in filter(bool, args)]
        if len(exceptions) > 0:
            logging.error(f"Exceptions returned during writing")
            for exc in exceptions:
                logging.error(exc)
        self.close()
        if not self.print_pydss:
            sys.stdout = sys.__stdout__

    def __handle_per_cum_date_info(self, dt64: np.datetime64) -> Tuple[str, str]:
        end_dt = datetime.datetime.utcfromtimestamp(
            (dt64 - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
        )
        start_dt = end_dt - datetime.timedelta(hours=1)
        path_d = start_dt.strftime("%d%b%Y:%H%M").upper()
        if end_dt.hour == 0 and end_dt.minute == 0:
            path_e = start_dt.strftime("%d%b%Y:2400").upper()
        else:
            path_e = end_dt.strftime("%d%b%Y:%H%M").upper()
        return path_d, path_e

    def __handle_inst_val_date_info(self, dt64: np.datetime64) -> Tuple[str, str]:
        start_dt = datetime.datetime.utcfromtimestamp(
            (dt64 - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s")
        )
        if start_dt.hour == 0 and start_dt.minute == 0:
            start_dt -= datetime.timedelta(days=1)
            path_d = start_dt.strftime("%d%b%Y:2400").upper()
        else:
            path_d = start_dt.strftime("%d%b%Y:%H%M").upper()
        path_e = ""
        return path_d, path_e

    def open(self):
        self.dss_file = Open(self.filepath)

    def close(self):
        self.dss_file.close()

    def write_data(self, xdata: xr.Dataset, labeled_variable_dict: Tuple[str, Dict[str, str]]):
        logging.debug("Writing data to dss")
        if not self.dss_file:
            raise ValueError(
                f"No dss file is open to receive data. Make sure that either context manager or open method is used before writing."
            )
        variable_dict = labeled_variable_dict[1]
        variable_name = variable_dict.get("variable")
        measurement_type = variable_dict.get("measurement")
        measurement_unit = variable_dict.get("unit")

        # Calculate coordinates not filled with nodata values and create spatial transform
        xdata = xdata.rio.reproject(SHG_WKT, resolution=self.resolution)
        xdata = xdata.where(xdata[variable_name] != xdata[variable_name].rio.nodata)
        if xdata.y.to_numpy()[-1] < xdata.y.to_numpy()[0]:
            y_coord = xdata.y.to_numpy()[-1]
        else:
            y_coord = xdata.y.to_numpy()[0]

        if xdata.x.to_numpy()[-1] < xdata.x.to_numpy()[0]:
            x_coord = xdata.x.to_numpy()[-1]
        else:
            x_coord = xdata.x.to_numpy()[0]
        affine_transform = Affine(self.resolution, 0.0, x_coord, 0.0, self.resolution, y_coord)

        # Get datetime and projection information from dataset
        wkt = xdata.rio.crs.wkt
        dt64 = xdata.time.to_numpy()[0]

        # Convert dataset to numpy array
        data = xdata[variable_name].isel(time=0).to_numpy()

        # Get transformation data and reassign infinite values to numpy nan values
        data[~np.isfinite(data)] = np.nan
        lower_left_x, lower_left_y = lower_left_xy_from_transform(
            affine_transform, data.shape, self.cell_zero_xcoord, self.cell_zero_ycoord
        )

        # Handle timestamps
        if measurement_type == "per-cum":
            path_d, path_e = self.__handle_per_cum_date_info(dt64)
        elif measurement_type == "inst-val":
            path_d, path_e = self.__handle_inst_val_date_info(dt64)
        else:
            raise NotImplementedError(f"Handling method not implemented for measurement type {measurement_type}")
        path = f"/{self.a}/{self.b}/{labeled_variable_dict[0]}/{path_d}/{path_e}/{self.f}/"

        grid_info = gridInfo()
        grid_info.update(
            [
                ("grid_type", self.grid_type),
                ("grid_crs", wkt),
                ("grid_transform", affine_transform),
                ("data_type", measurement_type),
                ("data_units", measurement_unit),
                ("opt_crs_name", "WKT"),
                ("opt_crs_type", 0),
                ("opt_compression", "zlib deflate"),
                ("opt_dtype", data.dtype),
                ("opt_grid_origin", "top-left corner"),
                ("opt_data_source", ""),
                ("opt_tzid", ""),
                ("opt_tzoffset", 0),
                ("opt_is_interval", False),
                ("opt_time_stamped", False),
                ("opt_lower_left_x", lower_left_x),
                ("opt_lower_left_y", lower_left_y),
                ("opt_cell_zero_xcoord", self.cell_zero_xcoord),
                ("opt_cell_zero_ycoord", self.cell_zero_ycoord),
            ]
        )

        self.dss_file.put_grid(path, data, grid_info)
        self.records += 1


def decode_dss_meta_json(input_json) -> DSSProductMeta:
    model_extent_name = input_json["model_extent_name"]
    model_extent_geojson_s3_uri = input_json["model_extent_geojson_s3_uri"]
    dss_s3_uri = input_json["dss_s3_uri"]
    start_dt = datetime.datetime.fromisoformat(input_json["start_date"])
    end_dt = datetime.datetime.fromisoformat(input_json["end_date"])
    last_modification = datetime.datetime.fromisoformat(input_json["last_modification"])
    sample_pathnames = input_json["sample_pathnames"]
    data_variables = input_json["data_variables"]
    shg_x = input_json["shg_x"]
    shg_y = input_json["shg_y"]
    overall_rank = input_json["overall_rank"]
    rank_within_year = input_json["rank_within_year"]
    overall_limit = input_json["overall_limit"]
    top_year_limit = input_json["top_year_limit"]
    meta = DSSProductMeta(
        model_extent_name,
        model_extent_geojson_s3_uri,
        dss_s3_uri,
        start_dt,
        end_dt,
        last_modification,
        data_variables,
        sample_pathnames,
        shg_x,
        shg_y,
        overall_rank,
        rank_within_year,
        overall_limit,
        top_year_limit,
    )
    return meta


def write_dss(
    xdata: xr.Dataset,
    data_variable_dict: Dict[str, Dict[str, str]],
    dss_path: str,
    path_a: str,
    path_b: str,
    path_f: str,
    resolution: int,
) -> None:
    with DSSWriter(dss_path, path_a, path_b, path_f, resolution) as writer:
        for labeled_variable_dict in data_variable_dict.items():
            for i, _ in enumerate(xdata.time.to_numpy()):
                data = xdata.isel(time=i)
                writer.write_data(data, labeled_variable_dict)
