from dataclasses import dataclass
from enum import Enum

from .dss import DSSMeasurementType, DSSUnit


@dataclass
class AORCVariableMeta:
    name: str
    dss_label: str
    dss_measurement_type: DSSMeasurementType
    dss_unit: DSSUnit


class PlaceholderAORCVariableMeta:
    def __init__(self, name: str):
        self.name = name

    @property
    def dss_label(self) -> str:
        raise NotImplementedError(f"AORC variable {self.name} does not have DSS parameters implemented")

    @property
    def dss_measurement_type(self) -> DSSMeasurementType:
        raise NotImplementedError(f"AORC variable {self.name} does not have DSS parameters implemented")

    @property
    def dss_unit(self) -> DSSUnit:
        raise NotImplementedError(f"AORC variable {self.name} does not have DSS parameters implemented")


class AORCVariable(Enum):
    APCP_SURFACE = AORCVariableMeta("APCP_surface", "PRECIPITATION", DSSMeasurementType.CUMULATIVE, DSSUnit.MILLIMETER)
    DLWRF_SURFACE = PlaceholderAORCVariableMeta("DLWRF_surface")
    DSWRF_SURFACE = PlaceholderAORCVariableMeta("DSWRF_surface")
    PRES_SURFACE = PlaceholderAORCVariableMeta("PRES_surface")
    SPFH_2MABOVEGROUND = PlaceholderAORCVariableMeta("SPFH_2maboveground")
    TMP_2MABOVEGROUND = AORCVariableMeta(
        "TMP_2maboveground", "TEMPERATURE", DSSMeasurementType.INSTANTANEOUS, DSSUnit.DEGREES_CELCIUS
    )
    UGRD_10MABOVEGROUND = PlaceholderAORCVariableMeta("UGRD_10maboveground")
    VGRD_10MABOVEGROUND = PlaceholderAORCVariableMeta("VGRD_10maboveground")


def str_to_aorc_variable(input_str: str) -> AORCVariable:
    try:
        return AORCVariable[input_str]
    except KeyError:
        for var in AORCVariable:
            if var.value.name == input_str:
                return var
    raise ValueError(f"Provided string {input_str} could not be parsed as AORCVariable")
