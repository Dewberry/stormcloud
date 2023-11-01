from dataclasses import dataclass
from datetime import date, datetime
from json import dumps

# from meilisearch import Client
from typing import List

from dataclasses_json import dataclass_json

from storms.transpose import Transpose


@dataclass_json
@dataclass
class _StormDocumentDateTime:
    datetime: str
    timestamp: int
    calendar_year: int
    water_year: int
    season: str


@dataclass_json
@dataclass
class _StormDocumentStats:
    count: int
    mean: float
    max: float
    min: float
    sum: float
    norm_mean: float


@dataclass_json
@dataclass
class _StormDocumentGeom:
    # indexes: List[List[int]]
    x_delta: int
    y_delta: int
    center_x: float
    center_y: float
    # area: float


@dataclass_json
@dataclass
class _StormDocumentMetaData:
    source: str
    watershed_name: str
    transposition_domain_name: str
    watershed_source: str
    transposition_domain_source: str
    # files: List[str]
    create_time: int


@dataclass_json
@dataclass
class _StormDocumentRanks:
    mean_rank: int
    max_rank: int
    norm_2yr_rank: int


@dataclass_json
@dataclass
class StormDocument:
    start: _StormDocumentDateTime
    duration: int
    stats: _StormDocumentStats
    metadata: _StormDocumentMetaData
    geom: _StormDocumentGeom
    # ranks: _StormDocumentRanks

    def write_to(self, file_path: str):
        with open(file_path, "w") as f:
            f.write(dumps(self.to_dict()))


def _format_datetime(event_start: datetime) -> _StormDocumentDateTime:
    str_format = str(event_start)
    timestamp = event_start.strftime("%s")
    calendar_year = event_start.year

    if event_start.month >= 10:
        water_year = calendar_year + 1
    else:
        water_year = calendar_year

    Y = 2000  # dummy leap year to allow input X-02-29 (leap day)
    seasons = [
        ("winter", (date(Y, 1, 1), date(Y, 3, 20))),
        ("spring", (date(Y, 3, 21), date(Y, 6, 20))),
        ("summer", (date(Y, 6, 21), date(Y, 9, 22))),
        ("autumn", (date(Y, 9, 23), date(Y, 12, 20))),
        ("winter", (date(Y, 12, 21), date(Y, 12, 31))),
    ]

    date_obj = event_start.date()
    dat = date_obj.replace(year=Y)

    season = next(season for season, (start, end) in seasons if start <= dat <= end)

    return _StormDocumentDateTime(
        datetime=str_format,
        timestamp=int(timestamp),
        calendar_year=calendar_year,
        water_year=water_year,
        season=season,
    )


def tranpose_to_doc(
    event_start: datetime,
    duration: int,
    watershed_name: str,
    domain_name: str,
    watershed_uri: str,
    domain_uri: str,
    transpose: Transpose,
):
    """
    Converts a transposer and transpose to a meilisearch document
    """

    storm_datetime = _format_datetime(event_start)

    storm_stats = _StormDocumentStats(
        count=transpose.count,
        mean=transpose.mean,
        max=transpose.max,
        min=transpose.min,
        sum=transpose.sum,
        norm_mean=transpose.normalized_mean,
    )

    storm_center = transpose.center
    storm_geom = _StormDocumentGeom(
        x_delta=transpose.x_delta,
        y_delta=transpose.y_delta,
        center_x=storm_center[0],
        center_y=storm_center[1],
    )

    storm_metadata = _StormDocumentMetaData(
        source="AORC",
        watershed_name=watershed_name,
        transposition_domain_name=domain_name,
        watershed_source=watershed_uri,
        transposition_domain_source=domain_uri,
        create_time=str(datetime.now()),
    )

    return StormDocument(
        start=storm_datetime,
        duration=duration,
        stats=storm_stats,
        metadata=storm_metadata,
        geom=storm_geom,
    )
