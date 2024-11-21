import datetime
import logging
import os
import sys
from enum import Enum
from typing import Iterator

import numpy as np
import rasterio.windows
import xarray as xr
from pydsstools.heclib.dss.HecDss import Open
from pydsstools.heclib.utils import (
    SHG_WKT,
    dss_logging,
    gridInfo,
    lower_left_xy_from_transform,
)

logging.root.setLevel(logging.ERROR)
dss_logging.config(level="Error")
logging.root.setLevel(logging.INFO)


class DSSUnit(Enum):
    MILLIMETER = "MM"
    DEGREES_CELCIUS = "DEG C"


class DSSMeasurementType(Enum):
    CUMULATIVE = "per-cum"
    INSTANTANEOUS = "inst-val"


def np_datetime64_to_datetime(dt64: np.datetime64) -> datetime.datetime:
    unix_epoch = np.datetime64(0, "s")
    one_second = np.timedelta64(1, "s")
    seconds_since_epoch = (dt64 - unix_epoch) / one_second
    dt = datetime.datetime.fromtimestamp(seconds_since_epoch, datetime.timezone.utc)
    return dt


class DSSFileWriter:
    def __init__(
        self, fn: str, path_a: str, path_b: str, path_f: str, verbose: bool = False, log_level: int = logging.INFO
    ):
        self.fn = fn
        self.path_a = path_a
        self.path_b = path_b
        self.path_f = path_f
        self.verbose = verbose
        self.file = None
        self.logger = self._create_logger(log_level)
        self._n_records = 0

    def __enter__(self):
        if not self.verbose:
            # supress printing from pydsstools
            sys.stdout = open(os.devnull, "w")
        self.open()
        return self

    def __exit__(self, *args) -> None:
        self.close()
        if not self.verbose:
            sys.stdout.close()
            sys.stdout = sys.__stdout__

    def open(self) -> None:
        self.file = Open(self.fn)

    def close(self) -> None:
        self.file.close()

    def _create_logger(self, log_level: int) -> logging.Logger:
        logger = logging.Logger(os.path.basename(self.fn) + "_logger")
        formatter = logging.Formatter("%(asctime)s -- %(levelname)s: %(message)s")
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)
        return logger

    def _handle_dt(
        self, dt: datetime.datetime | np.datetime64, measurement_type: DSSMeasurementType
    ) -> tuple[str, str]:
        if measurement_type == DSSMeasurementType.CUMULATIVE:
            return self.__handle_cumulative_dt(dt)
        elif measurement_type == DSSMeasurementType.INSTANTANEOUS:
            return self.__handle_instantaneous_dt(dt)
        raise NotImplementedError(f"No datetime handler implemented for measurement type {measurement_type}")

    @staticmethod
    def _get_grid_type(measurement_type: DSSMeasurementType) -> str:
        if measurement_type == DSSMeasurementType.CUMULATIVE:
            return "shg-time"
        elif measurement_type == DSSMeasurementType.INSTANTANEOUS:
            # throws error if path e is empty string and grid type is 'shg-time'
            return "specified"
        raise NotImplementedError(f"No grid type handler implemented for measurement type {measurement_type}")

    @staticmethod
    def __handle_cumulative_dt(dt: datetime.datetime | np.datetime64) -> tuple[str, str]:
        if isinstance(dt, np.datetime64):
            dt = np_datetime64_to_datetime(dt)
        start_dt = dt - datetime.timedelta(hours=1)
        path_d = start_dt.strftime("%d%b%Y:%H%M").upper()
        if dt.hour == 0 and dt.minute == 0:
            path_e = start_dt.strftime("%d%b%Y:2400").upper()
        else:
            path_e = dt.strftime("%d%b%Y:%H%M").upper()
        return path_d, path_e

    @staticmethod
    def __handle_instantaneous_dt(dt: datetime.datetime | np.datetime64) -> tuple[str, str]:
        if isinstance(dt, np.datetime64):
            dt = np_datetime64_to_datetime(dt)
        if dt.hour == 0 and dt.minute == 0:
            dt -= datetime.timedelta(days=1)
            path_d = dt.strftime("%d%b%Y:2400").upper()
        else:
            path_d = dt.strftime("%d%b%Y:%H%M").upper()
        path_e = ""
        return path_d, path_e

    @property
    def n_records(self) -> int:
        return self._n_records

    def create_dss_metadata_from_xr_dataset(
        self,
        dataset: xr.Dataset,
        variable_list: list[tuple[str, str, DSSUnit, DSSMeasurementType]],
        clip_to_data: bool = False,
    ) -> Iterator[tuple[str, np.ndarray, gridInfo]]:
        for time_slice in dataset.time:
            dt64 = time_slice.values
            ds = dataset.sel(time=time_slice)
            ds = ds.rio.reproject(SHG_WKT)
            for dataset_label, dss_label, unit, measurement_type in variable_list:
                data = ds[dataset_label]
                data_np = data.to_numpy()
                if clip_to_data:
                    data_mask = np.isfinite(data_np)
                    data = data.rio.isel_window(rasterio.windows.get_data_window(data_mask, ~data_mask))
                    data_np = data.to_numpy()
                transform = data.rio.transform(recalc=True)
                lower_left_x, lower_left_y = lower_left_xy_from_transform(transform, data_np.shape, 0, 0)
                grid_type = self._get_grid_type(measurement_type)
                path_d, path_e = self._handle_dt(dt64, measurement_type)
                path = "/".join([self.path_a, self.path_b, dss_label, path_d, path_e, self.path_f])
                path = f"/{path}/"
                grid_info = gridInfo()
                grid_info.update(
                    [
                        ("grid_type", grid_type),
                        ("grid_crs", SHG_WKT),
                        ("grid_transform", transform),
                        ("data_type", measurement_type.value),
                        ("data_units", unit.value),
                        ("opt_crs_name", "WKT"),
                        ("opt_crs_type", 0),
                        ("opt_compression", "zlib deflate"),
                        ("opt_dtype", data_np.dtype),
                        ("opt_grid_origin", "top-left corner"),
                        ("opt_data_source", ""),
                        ("opt_tzid", ""),
                        ("opt_tzoffset", 0),
                        ("opt_is_interval", False),
                        ("opt_timestamped", False),
                        ("opt_lower_left_x", lower_left_x),
                        ("opt_lower_left_y", lower_left_y),
                        ("opt_cell_zero_xcoord", 0),
                        ("opt_cell_zero_ycoord", 0),
                    ]
                )
                yield path, data_np, grid_info

    def write_from_xr_dataset(
        self,
        dataset: xr.Dataset,
        variable_list: list[tuple[str, str, DSSUnit, DSSMeasurementType]],
        clip_to_data: bool = False,
    ) -> list[tuple[str, np.ndarray, gridInfo]]:
        meta_list = []
        if self.file == None:
            raise ValueError(f"No file is open to receive data")
        for path, data, grid_info in self.create_dss_metadata_from_xr_dataset(dataset, variable_list, clip_to_data):
            self.file.put_grid(path, data, grid_info)
            self._n_records += 1
            meta_list.append((path, data, grid_info))
        return meta_list
