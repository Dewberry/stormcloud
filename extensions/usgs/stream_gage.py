from datetime import datetime
from typing import Literal, Optional, Union

import pystac
from pystac.extensions.base import ExtensionManagementMixin, PropertiesExtension
from pystac.item import Item

USGS_EXTENSION_URI = (
    "https://raw.githubusercontent.com/dewberry/stormcloud/refs/heads/feature/gages/extensions/usgs/schema.json"
)

PREFIX: str = "sgage"


def extension_key(key: str) -> str:
    return f"{PREFIX}:{key}"


GAGE_NUMBER_PROP: str = extension_key("gage_number")
GAGE_NAME_PROP: str = extension_key("gage_name")
STATE_PROP: str = extension_key("state")
HUC_PROP: str = extension_key("huc")
DRAINAGE_AREA_PROP: str = extension_key("drainage_area")
ALTITUDE_PROP: str = extension_key("altitude")
VERTICAL_DATUM_PROP: str = extension_key("vertical_datum")
TIME_ZONE_PROP: str = extension_key("time_zone")
PEAKS_START_PROP: str = extension_key("peaks_start")
PEAKS_END_PROP: str = extension_key("peaks_end")
DAILY_START_PROP: str = extension_key("daily_start")
DAILY_END_PROP: str = extension_key("daily_end")
DAILY_YEARS_OF_RECORD_PROP: str = extension_key("daily_years")
INSTANTANEOUS_START_PROP: str = extension_key("iv_start")
INSTANTANEOUS_END_PROP: str = extension_key("iv_end")
INSTANTANEOUS_YEARS_OF_RECORD_PROP: str = extension_key("iv_years")
MONTHLY_MEAN_FLOW: str = extension_key("monthly_mean_flow")


class StreamGage(
    PropertiesExtension,
    ExtensionManagementMixin[Union[pystac.Item, pystac.Asset]],
):
    """An abstract class that can be used to extend the properties of an
    :class:`~pystac.Item` with properties from the USGS Extension."""

    def __init__(self, item: Item):
        self.item = item
        self.properties = item.properties

    name: Literal["usgs"] = "usgs"

    def apply(
        self,
        gage_number: str,
        gage_name: str,
        state: str,
        huc: str,
        drainage_area: float,
        altitude: float,
        vertical_datum: str,
        time_zone: str,
        peaks_start: Optional[Union[str, datetime]] = None,
        peaks_end: Optional[Union[str, datetime]] = None,
        daily_start: Optional[Union[str, datetime]] = None,
        daily_end: Optional[Union[str, datetime]] = None,
        daily_years_of_record: Optional[float] = None,
        instantaneous_end: Optional[Union[str, datetime]] = None,
        instantaneous_years_of_record: Optional[float] = None,
    ) -> None:
        """Applies USGS extension properties to the extended Item."""
        self.gage_number = gage_number
        self.gage_name = gage_name
        self.state = state
        self.huc = huc
        self.drainage_area = drainage_area
        self.altitude = altitude
        self.vertical_datum = vertical_datum
        self.time_zone = time_zone
        self.peaks_start = peaks_start
        self.peaks_end = peaks_end
        self.daily_start = daily_start
        self.daily_end = daily_end
        self.daily_years_of_record = daily_years_of_record
        self.instantaneous_end = instantaneous_end
        self.instantaneous_years_of_record = instantaneous_years_of_record

    # Getter and Setter for each property
    @property
    def gage_number(self) -> str:
        return self._get_property(GAGE_NUMBER_PROP, str)

    @gage_number.setter
    def gage_number(self, v: str) -> None:
        self._set_property(GAGE_NUMBER_PROP, v, pop_if_none=False)

    @property
    def gage_name(self) -> str:
        return self._get_property(GAGE_NAME_PROP, str)

    @gage_name.setter
    def gage_name(self, v: str) -> None:
        self._set_property(GAGE_NAME_PROP, v, pop_if_none=False)

    @property
    def state(self) -> str:
        return self._get_property(STATE_PROP, str)

    @state.setter
    def state(self, v: str) -> None:
        self._set_property(STATE_PROP, v, pop_if_none=False)

    @property
    def huc(self) -> str:
        return self._get_property(HUC_PROP, str)

    @huc.setter
    def huc(self, v: str) -> None:
        self._set_property(HUC_PROP, v, pop_if_none=False)

    @property
    def drainage_area(self) -> float:
        return self._get_property(DRAINAGE_AREA_PROP, float)

    @drainage_area.setter
    def drainage_area(self, v: float) -> None:
        self._set_property(DRAINAGE_AREA_PROP, v, pop_if_none=False)

    @property
    def altitude(self) -> float:
        return self._get_property(ALTITUDE_PROP, float)

    @altitude.setter
    def altitude(self, v: float) -> None:
        self._set_property(ALTITUDE_PROP, v, pop_if_none=False)

    @property
    def vertical_datum(self) -> str:
        return self._get_property(VERTICAL_DATUM_PROP, str)

    @vertical_datum.setter
    def vertical_datum(self, v: str) -> None:
        self._set_property(VERTICAL_DATUM_PROP, v, pop_if_none=False)

    @property
    def time_zone(self) -> str:
        return self._get_property(TIME_ZONE_PROP, str)

    @time_zone.setter
    def time_zone(self, v: str) -> None:
        self._set_property(TIME_ZONE_PROP, v, pop_if_none=False)

    @property
    def peaks_start(self) -> Optional[Union[str, datetime]]:
        return self._get_property(PEAKS_START_PROP, Union[str, datetime])

    @peaks_start.setter
    def peaks_start(self, v: Optional[Union[str, datetime]]) -> None:
        self._set_property(PEAKS_START_PROP, v.isoformat() if isinstance(v, datetime) else v)

    @property
    def peaks_end(self) -> Optional[Union[str, datetime]]:
        return self._get_property(PEAKS_END_PROP, Union[str, datetime])

    @peaks_end.setter
    def peaks_end(self, v: Optional[Union[str, datetime]]) -> None:
        self._set_property(PEAKS_END_PROP, v.isoformat() if isinstance(v, datetime) else v)

    @property
    def daily_start(self) -> Optional[Union[str, datetime]]:
        return self._get_property(DAILY_START_PROP, Union[str, datetime])

    @daily_start.setter
    def daily_start(self, v: Optional[Union[str, datetime]]) -> None:
        self._set_property(DAILY_START_PROP, v.isoformat() if isinstance(v, datetime) else v)

    @property
    def daily_end(self) -> Optional[Union[str, datetime]]:
        return self._get_property(DAILY_END_PROP, Union[str, datetime])

    @daily_end.setter
    def daily_end(self, v: Optional[Union[str, datetime]]) -> None:
        self._set_property(DAILY_END_PROP, v.isoformat() if isinstance(v, datetime) else v)

    @property
    def daily_years_of_record(self) -> Optional[float]:
        return self._get_property(DAILY_YEARS_OF_RECORD_PROP, float)

    @daily_years_of_record.setter
    def daily_years_of_record(self, v: Optional[float]) -> None:
        self._set_property(DAILY_YEARS_OF_RECORD_PROP, v)

    @property
    def instantaneous_end(self) -> Optional[Union[str, datetime]]:
        return self._get_property(INSTANTANEOUS_END_PROP, Union[str, datetime])

    @instantaneous_end.setter
    def instantaneous_end(self, v: Optional[Union[str, datetime]]) -> None:
        self._set_property(INSTANTANEOUS_END_PROP, v.isoformat() if isinstance(v, datetime) else v)

    @property
    def instantaneous_years_of_record(self) -> Optional[float]:
        return self._get_property(INSTANTANEOUS_YEARS_OF_RECORD_PROP, float)

    @instantaneous_years_of_record.setter
    def instantaneous_years_of_record(self, v: Optional[float]) -> None:
        self._set_property(INSTANTANEOUS_YEARS_OF_RECORD_PROP, v)

    # @property
    # def monthly_mean_flow(self) -> Optional[float]:
    #     return self._get_property(INSTANTANEOUS_YEARS_OF_RECORD_PROP, float)

    # @monthly_mean_flow.setter
    # def monthly_mean_flow(self, v: Optional[float]) -> None:
    #     self._set_property(INSTANTANEOUS_YEARS_OF_RECORD_PROP, v)

    @classmethod
    def get_schema_uri(cls) -> str:
        return USGS_EXTENSION_URI

    @classmethod
    def ext(cls, item: Item) -> "StreamGage":
        """Return an instance of StreamGage for the given item."""
        if USGS_EXTENSION_URI not in item.stac_extensions:
            item.stac_extensions.append(USGS_EXTENSION_URI)
        return cls(item)
