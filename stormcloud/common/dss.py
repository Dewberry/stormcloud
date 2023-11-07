import logging
import os
import sys
import warnings
from datetime import datetime, timedelta
from typing import Dict

import numpy as np
from affine import Affine
from pydsstools.heclib.dss.HecDss import Open
from pydsstools.heclib.utils import (
    SHG_WKT,
    dss_logging,
    gridInfo,
    lower_left_xy_from_transform,
)

warnings.filterwarnings("ignore")

import rioxarray as rxr
import xarray as xr

# quietly set dss_logging to level ERROR
# without setting the root logger to ERROR will write warning when changing
logging.root.setLevel(logging.ERROR)
dss_logging.config(level="Error")
logging.root.setLevel(logging.INFO)


def write_multivariate_dss(
    xdata: xr.Dataset,
    data_variable_dict: Dict[str, Dict[str, str]],
    dss_path: str,
    path_a: str,
    path_b: str,
    path_f: str,
    resolution: int,
):
    logging.info(f"Writing DSS file to {dss_path}")
    # Get variables which are constant to dataset
    xdata = xdata.rio.reproject(SHG_WKT, resolution=resolution)
    grid_type = "shg-time"
    cell_zero_xcoord = 0
    cell_zero_ycoord = 0

    # Iterate through data variables
    for label, variable_dict in data_variable_dict.items():
        variable_name = variable_dict.get("variable")
        measurement_type = variable_dict.get("measurement")
        measurement_unit = variable_dict.get("unit")
        # Calculate coordinates not filled with nodata values and create spatial transform
        xdata = xdata.where(xdata[variable_name] != xdata[variable_name].rio.nodata)
        if xdata.y.to_numpy()[-1] < xdata.y.to_numpy()[0]:
            y_coord = xdata.y.to_numpy()[-1]
        else:
            y_coord = xdata.y.to_numpy()[0]

        if xdata.x.to_numpy()[-1] < xdata.x.to_numpy()[0]:
            x_coord = xdata.x.to_numpy()[-1]
        else:
            x_coord = xdata.x.to_numpy()[0]
        affine_transform = Affine(resolution, 0.0, x_coord, 0.0, resolution, y_coord)
        wkt = xdata.rio.crs.wkt

        # Suppress printing from pydsstools
        sys.stdout = open(os.devnull, "w")  # prevent prints to stdout

        # Write pathnames into file
        with Open(dss_path) as fid:
            for i, dt64 in enumerate(xdata.time.to_numpy()):
                data = xdata.isel(time=i)[variable_name].to_numpy()
                data[~np.isfinite(data)] = np.nan
                if i == 0:
                    lower_left_x, lower_left_y = lower_left_xy_from_transform(
                        affine_transform, data.shape, cell_zero_xcoord, cell_zero_ycoord
                    )
                if measurement_type == "per-cum":
                    end_dt = datetime.utcfromtimestamp(
                        (dt64 - np.datetime64("1970-01-01T00:00:00"))
                        / np.timedelta64(1, "s")
                    )
                    start_dt = end_dt - timedelta(hours=1)
                    path_d = start_dt.strftime("%d%b%Y:%H%M").upper()
                    if end_dt.hour == 0 and end_dt.minute == 0:
                        path_e = start_dt.strftime("%d%b%Y:2400").upper()
                    else:
                        path_e = end_dt.strftime("%d%b%Y:%H%M").upper()
                elif measurement_type == "inst-val":
                    start_dt = datetime.utcfromtimestamp(
                        (dt64 - np.datetime64("1970-01-01T00:00:00"))
                        / np.timedelta64(1, "s")
                    )
                    path_d = start_dt.strftime("%d%b%Y:%H%M").upper()
                    path_e = ""
                else:
                    raise NotImplementedError(
                        f"Handling method not implemented for measurement type {measurement_type}"
                    )
                path = f"/{path_a}/{path_b}/{label}/{path_d}/{path_e}/{path_f}/"

                grid_info = gridInfo()
                grid_info.update(
                    [
                        ("grid_type", grid_type),
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
                        ("opt_cell_zero_xcoord", cell_zero_xcoord),
                        ("opt_cell_zero_ycoord", cell_zero_ycoord),
                    ]
                )

                fid.put_grid(path, data, grid_info)

        sys.stdout = sys.__stdout__  # reenable prints to stdout
