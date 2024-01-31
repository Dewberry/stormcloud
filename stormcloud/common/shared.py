import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Union


@dataclass
class MSStormResult:
    id: str
    start_date: str
    duration: int
    watershed: str
    create_time: str
    center_x: float
    center_y: float
    declustered_rank: int
    filter_year: Union[int, None] = None


class NOAADataVariable(Enum):
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
            return "DEG C"
        else:
            raise NotImplementedError(f"Unit unknown for data variable {self.__repr__}")


class DSSVariable(Enum):
    TEMPERATURE = "Temperature"
    PRECIPITATION = "Precipitation"


@dataclass
class DSSPathnameMeta:
    resolution: int
    watershed: str
    start_dt: datetime.datetime
    end_dt: Union[datetime.datetime, None]
    source: str
    grid_variable: DSSVariable


def convert_noaa_var_to_dss_var(noaa_var: NOAADataVariable) -> DSSVariable:
    if noaa_var.name == "APCP":
        return DSSVariable.PRECIPITATION
    if noaa_var.name == "TMP":
        return DSSVariable.TEMPERATURE
    else:
        return NotImplementedError(f"DSS translation of {noaa_var.name} not available")


def decode_data_variable(data_variable: str) -> DSSVariable:
    for e in DSSVariable:
        if data_variable == e.name:
            return e
    raise ValueError(f"Expected one of {', '.join([e.name for e in DSSVariable])} -- got {data_variable}")
