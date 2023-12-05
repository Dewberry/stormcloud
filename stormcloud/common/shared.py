import datetime
from dataclasses import dataclass
from enum import Enum
from typing import Union


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


def decode_data_variable(data_variable: str) -> DSSVariable:
    for e in DSSVariable:
        if data_variable == e.name:
            return e
    raise ValueError(f"Expected one of {', '.join([e.name for e in DSSVariable])} -- got {data_variable}")
