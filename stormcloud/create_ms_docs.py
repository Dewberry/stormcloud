import datetime
import logging
from dataclasses import dataclass
from types import NoneType
from typing import Any, Iterator, List, Tuple, Union

import numpy as np
from scipy.stats import rankdata


@dataclass
class SSTStart:
    datetime: str
    timestamp: int
    calendar_year: int
    water_year: int
    season: str


@dataclass
class SSTStats:
    count: int
    mean: float
    max: float
    min: float
    sum: float
    norm_mean: Union[float, NoneType]


@dataclass
class SSTGeom:
    x_delta: int
    y_delta: int
    center_x: float
    center_y: float


@dataclass
class SSTMeta:
    source: str
    watershed_name: str
    transposition_domain_name: str
    watershed_source: str
    transposition_domain_source: str
    create_time: str


@dataclass
class SSTS3Document:
    start: SSTStart
    duration: int
    stats: SSTStats
    metadata: SSTMeta
    geom: SSTGeom

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        d = {
            "start": self.start.__dict__,
            "duration": self.duration,
            "stats": self.stats.__dict__,
            "metadata": self.metadata.__dict__,
            "geom": self.geom.__dict__,
        }
        for k, v in d.items():
            yield k, v


@dataclass
class TropicalStorm:
    id: str
    name: str
    start: str
    end: str
    nature: str


class SSTMSDocument:
    def __init__(
        self,
        s3_document: SSTS3Document,
        png_bucket: str,
        true_rank: int,
        declustered_rank: int,
        storm_json: Union[List[dict], NoneType],
    ) -> None:
        self.start = s3_document.start
        self.duration = s3_document.duration
        self.stats = s3_document.stats
        self.start_dt = datetime.datetime.strptime(self.start.datetime, "%Y-%m-%d %H:%M:%S")
        self.end_dt = self.start_dt + datetime.timedelta(hours=self.duration)
        self.id = self._create_id(s3_document.metadata)
        self.categories = self._create_categories(s3_document.metadata)
        self.rank_dict = self._create_ranks(true_rank, declustered_rank)
        self.tropical_storms = self._get_tropical_storm_dicts(storm_json)
        self.metadata = self._add_png_meta(s3_document.metadata, png_bucket)

    def _create_id(self, s3_meta: SSTMeta) -> str:
        meta_id = f"{sanitize_for_s3(s3_meta.watershed_name)}_{sanitize_for_s3(s3_meta.transposition_domain_name)}_{self.duration}h_{self.start_dt.strftime('%Y%m%d')}"
        return meta_id

    def _create_categories(self, s3_meta: SSTMeta) -> dict:
        categories = {
            "lv10": s3_meta.watershed_name,
            "lv11": f"{s3_meta.watershed_name} > {s3_meta.transposition_domain_name}",
        }
        return categories

    @staticmethod
    def _create_ranks(true_rank: int, declustered_rank: int) -> dict:
        ranks = {"true_rank": true_rank, "declustered_rank": declustered_rank}
        return ranks

    def _get_tropical_storm_dicts(self, storm_json: Union[List[dict], NoneType]) -> Union[List[dict], NoneType]:
        if storm_json:
            ts_list = lookup_storms(self.start_dt, self.end_dt, storm_json)
            ts_dict_list = [ts.__dict__ for ts in ts_list]
            return ts_dict_list
        return None

    def _add_png_meta(self, s3_meta: SSTMeta, png_bucket: str) -> dict:
        meta_dict = s3_meta.__dict__
        meta_dict[
            "png"
        ] = f"https://{png_bucket}.s3.amazonaws.com/watersheds/{sanitize_for_s3(s3_meta.watershed_name)}/{sanitize_for_s3(s3_meta.watershed_name)}-transpo-area-{sanitize_for_s3(s3_meta.transposition_domain_name)}/{self.duration}h/pngs/{self.start_dt.strftime('%Y%m%d')}"
        return meta_dict

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        d = {
            "id": self.id,
            "start": self.start.__dict__,
            "duration": self.duration,
            "stats": self.stats.__dict__,
            "metadata": self.metadata,
            "categories": self.categories,
            "tropical_storms": self.tropical_storms,
            "rank": self.rank_dict,
        }
        for k, v in d.items():
            if k == "tropical_storms" and v == None:
                continue
            else:
                yield k, v


def lookup_storms(
    start_dt: datetime.datetime, end_dt: datetime.datetime, storm_json: List[dict]
) -> List[TropicalStorm]:
    logging.debug(f"searching for storms between {start_dt} and {end_dt}")
    ts_list = []
    for storm in storm_json:
        ts = TropicalStorm(**storm)
        ts_start_dt = datetime.datetime.strptime(ts.start, "%Y-%m-%d")
        ts_end_dt = datetime.datetime.strptime(ts.end, "%Y-%m-%d")
        latest_start = max(ts_start_dt, start_dt)
        earliest_end = min(ts_end_dt, end_dt)
        delta = earliest_end - latest_start
        if delta.total_seconds() > 0:
            ts_list.append(ts)
        if ts_list:
            logging.debug(f"{len(ts_list)} storm found between {start_dt} and {end_dt}: {ts_list}")
    return ts_list


def sanitize_for_s3(original_str: str) -> str:
    return original_str.replace(" ", "-").lower()


def create_ms_documents(
    data: List[SSTS3Document], png_bucket: str, storm_json: Union[List[dict], NoneType]
) -> List[SSTMSDocument]:
    # get values for attributes of interest for docs overall
    docs = np.array([dict(d) for d in data])
    starts = np.array([datetime.datetime.strptime(d["start"]["datetime"], "%Y-%m-%d %H:%M:%S") for d in docs])
    means = np.array([d["stats"]["mean"] for d in docs])
    mean_ranks = rankdata(means * -1, method="ordinal")

    # date decluster
    for i in range(1, len(mean_ranks) + 1):
        idx = np.where(mean_ranks == i)
        dt = starts[idx][0]
        if i == 1:
            decluster_mask = np.array([True])
            starts_by_mean = np.array([dt])
        else:
            min_dt = dt - datetime.timedelta(hours=71)
            max_dt = dt + datetime.timedelta(hours=71)
            if np.any((starts_by_mean[decluster_mask] >= min_dt) & (starts_by_mean[decluster_mask] <= max_dt)):
                decluster = False
            else:
                decluster = True
            decluster_mask = np.append(decluster_mask, decluster)
            starts_by_mean = np.append(starts_by_mean, dt)
    year_range = np.array([dt.year for dt in starts])
    years_by_mean = np.array([dt.year for dt in starts_by_mean])

    # get true ranks and declustered rank within each year of interest
    ranked_docs = []
    for year in year_range:
        yr_decluster_mask = decluster_mask[years_by_mean == year]
        yr_starts_by_mean = starts_by_mean[years_by_mean == year]
        yr_starts = starts[year_range == year]
        yr_docs = docs[year_range == year]

        for doc, start in zip(yr_docs, yr_starts):
            idx = np.where(yr_starts_by_mean == start)[0][0]
            if yr_decluster_mask[idx]:
                decluster_rank = np.where(yr_starts_by_mean[yr_decluster_mask] == start)[0][0] + 1
            else:
                decluster_rank = -1
            true_rank = idx + 1
            s3_doc = SSTS3Document(
                SSTStart(**doc["start"]),
                doc["duration"],
                SSTStats(**doc["stats"]),
                SSTMeta(**doc["metadata"]),
                SSTGeom(**doc["geom"]),
            )
            ms_doc = SSTMSDocument(s3_doc, png_bucket, int(true_rank), int(decluster_rank), storm_json)
            ranked_docs.append(ms_doc)
    return ranked_docs
