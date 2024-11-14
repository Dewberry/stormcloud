from typing import Any, Literal, Optional

from affine import Affine
from pystac import ExtensionTypeError, Item
from pystac.extensions.base import ExtensionManagementMixin, PropertiesExtension
from pystac.utils import StringEnum, get_required

AORC_SCHEMA_URI = "https://dewberry.github.io/sst-stac-extension/v0.1.0-beta/schema.json"
AORC_PREFIX = "aorc:"
AORC_STATISTICS_PROP = AORC_PREFIX + "statistics"
AORC_TRANSFORM_PROP = AORC_PREFIX + "transform"


class Unit(StringEnum):
    INCH = "in"
    MILLIMETER = "mm"


class AccumulationMeasurementWithUnits:
    def __init__(self, value: float, unit: Unit):
        self.value = value
        self.unit = unit

    def __str__(self) -> str:
        return f"<AccumulationMeasurementWithUnits value={self.value} unit={self.unit.value}>"

    def to_dict(self) -> dict[str, Any]:
        return {"value": self.value, "unit": self.unit.value}

    @classmethod
    def from_dict(cls, dictionary: dict[str, Any]) -> None:
        return cls(dictionary["value"], Unit(dictionary["unit"]))


class AORCStatistics:
    properties: dict[str, Any]

    def __init__(self, properties: dict[str, Any]):
        self.properties = properties

    @property
    def min(self) -> dict[str, Any]:
        return get_required(self.properties.get("min"), self, "min")

    @min.setter
    def min(self, min: AccumulationMeasurementWithUnits) -> None:
        proxy_dict = min.to_dict()
        self.properties["min"] = proxy_dict

    @property
    def mean(self) -> dict[str, Any]:
        return get_required(self.properties.get("mean"), self, "mean")

    @mean.setter
    def mean(self, mean: AccumulationMeasurementWithUnits) -> None:
        proxy_dict = mean.to_dict()
        self.properties["mean"] = proxy_dict

    @property
    def max(self) -> dict[str, Any]:
        return get_required(self.properties.get("max"), self, "max")

    @max.setter
    def max(self, max: AccumulationMeasurementWithUnits) -> None:
        proxy_dict = max.to_dict()
        self.properties["max"] = proxy_dict

    @property
    def count(self) -> int:
        return get_required(self.properties.get("count"), self, "count")

    @count.setter
    def count(self, count: int) -> None:
        self.properties["count"] = count

    @property
    def normalized_mean(self) -> float | None:
        return self.properties.get("normalized_mean")

    @normalized_mean.setter
    def normalized_mean(self, normalized_mean: float | None) -> None:
        if normalized_mean != None:
            self.properties["normalized_mean"] = normalized_mean

    def __repr__(self) -> str:
        return f"<Statistics min={self.min} mean={self.mean} max={self.max} count={self.count} normalized_mean={self.normalized_mean}>"

    def apply(
        self,
        min: AccumulationMeasurementWithUnits,
        mean: AccumulationMeasurementWithUnits,
        max: AccumulationMeasurementWithUnits,
        count: int,
        normalized_mean: Optional[float] = None,
    ) -> None:
        self.min = min
        self.mean = mean
        self.max = max
        self.count = count
        self.normalized_mean = normalized_mean

    @classmethod
    def create(
        cls,
        min: AccumulationMeasurementWithUnits,
        mean: AccumulationMeasurementWithUnits,
        max: AccumulationMeasurementWithUnits,
        count: int,
        normalized_mean: Optional[float] = None,
    ) -> "AORCStatistics":
        aorc_s = cls({})
        aorc_s.apply(min, mean, max, count, normalized_mean)
        return aorc_s

    def __dict__(self) -> dict[str, Any]:
        return self.properties

    def to_dict(self) -> dict[str, Any]:
        return self.properties


class AORCExtension(PropertiesExtension, ExtensionManagementMixin[Item]):
    name: Literal["aorc"] = "aorc"

    def __init__(self, item: Item):
        self.item = item
        self.properties = item.properties

    def apply(self, statistics: AORCStatistics, transform: list[float]) -> None:
        self.statistics = statistics
        self.transform = transform

    @property
    def statistics(self) -> dict[str, Any]:
        return get_required(self._get_property(AORC_STATISTICS_PROP, dict), self, AORC_STATISTICS_PROP)

    @statistics.setter
    def statistics(self, statistics: AORCStatistics) -> None:
        self._set_property(AORC_STATISTICS_PROP, statistics.to_dict())

    @property
    def transform(self) -> list[float]:
        return get_required(self._get_property(AORC_TRANSFORM_PROP, list), self, AORC_TRANSFORM_PROP)

    @transform.setter
    def transform(self, transform: Affine | list[float]) -> None:
        if not isinstance(transform, Affine):
            transform = Affine(*transform)
        self._set_property(
            AORC_TRANSFORM_PROP, [transform.a, transform.b, transform.c, transform.d, transform.e, transform.f]
        )

    @classmethod
    def get_schema_uri(cls) -> str:
        return AORC_SCHEMA_URI

    @classmethod
    def ext(cls, item: Item, add_if_missing: bool = True) -> "AORCExtension":
        if isinstance(item, Item):
            cls.ensure_has_extension(item, add_if_missing)
            return cls(item)
        else:
            raise ExtensionTypeError(f"SSTExtension does not apply to type '{type(item).__name__}")
