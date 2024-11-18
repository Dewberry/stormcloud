import datetime
from enum import Enum

import numpy as np
import xarray as xr


class DSSUnit(Enum):
    MILLIMETER = "MM"
    DEGREES_CELCIUS = "DEG C"


class DSSMeasurementType(Enum):
    CUMULATIVE = "per-cum"
    INSTANTANEOUS = "inst-val"


class DSSFileWriter:
    def __init__(self, fn: str, path_a: str, path_b: str, path_f: str):
        self.fn = fn
        self.path_a = path_a
        self.path_b = path_b
        self.path_f = path_f

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args) -> None:
        self.close()

    def open(self) -> None:
        pass

    def close(self) -> None:
        pass

    def _handle_dt(self, dt: datetime.datetime, measurement_type: DSSMeasurementType) -> tuple[str, str]:
        if measurement_type == DSSMeasurementType.CUMULATIVE:
            return self.__handle_cumulative_dt(dt)
        elif measurement_type == DSSMeasurementType.INSTANTANEOUS:
            return self.__handle_instantaneous_dt(dt)
        raise NotImplementedError(f"No datetime handler implemented for measurement type {measurement_type}")

    @staticmethod
    def __handle_cumulative_dt(dt: datetime.datetime) -> tuple[str, str]:
        pass

    @staticmethod
    def __handle_instantaneous_dt(dt: datetime.datetime) -> tuple[str, str]:
        pass

    def write_from_xr_dataset(
        self, dataset: xr.Dataset, variable_list: list[tuple[str, str, DSSUnit, DSSMeasurementType]]
    ) -> None:
        for dataset_label, dss_label, unit, measurement_type in variable_list:
            pass
