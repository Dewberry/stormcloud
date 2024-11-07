import datetime
from dataclasses import dataclass

import numpy as np
from pystac import CatalogType, Collection, Item


@dataclass
class CollectionMetrics:
    id_list: list[str]
    mean_list: list[float]
    start: datetime.datetime
    end: datetime.datetime


class Calendar:
    def __init__(
        self,
        start: datetime.datetime,
        end: datetime.datetime,
        calendar_interval: datetime.timedelta,
    ) -> None:
        self.start = start
        self.end = end
        self.interval = calendar_interval
        self.calendar_interval = calendar_interval
        self.datetime_array = self._create_datetime_array()

    def unblocked_dates(self) -> np.ndarray:
        return self.datetime_array.compressed()

    def _create_datetime_array(self) -> np.ma.MaskedArray:
        dt_list = []
        current_time = self.start
        while current_time < self.end:
            dt_list.append(np.datetime64(current_time))
            current_time += self.interval
        array = np.ma.array(data=dt_list, dtype=np.datetime64)
        return array

    def block_window(self, start: np.datetime64, end: np.datetime64) -> None:
        mask = (self.datetime_array >= start) & (self.datetime_array < end)
        self.datetime_array[mask] = np.ma.masked

    def block_if_available(self, start: datetime.datetime, end: datetime.datetime) -> bool:
        unblocked = self.unblocked_dates()
        np_start = np.datetime64(start)
        np_end = np.datetime64(end)
        if np_start in unblocked and np_end in unblocked:
            self.block_window(np_start, np_end)
            return True
        return False


def parse_item_time_range(item: Item) -> tuple[datetime.datetime, datetime.datetime]:
    item_start_dt = datetime.datetime.strptime(item.properties["start_datetime"], "%Y-%m-%dT%H:%M:%SZ")
    item_end_dt = datetime.datetime.strptime(item.properties["end_datetime"], "%Y-%m-%dT%H:%M:%SZ")
    if item_start_dt == item_end_dt:
        raise ValueError(f"Item {item.id} start ({item_start_dt}) and item end ({item_end_dt}) are the same")
    return item_start_dt, item_end_dt


def rank_ids(id_list: list[str], mean_list: list[float]) -> list[str]:
    id_array = np.array(id_list, dtype=np.dtypes.StrDType)
    mean_array = np.array(mean_list, dtype=np.float64)
    sorted_indexes = np.argsort(mean_array)[::-1]
    sorted_id_array = id_array[sorted_indexes]
    return sorted_id_array.tolist()


def collect_collection_metrics(collection: Collection) -> CollectionMetrics:
    id_list: list[str] = []
    mean_list: list[float] = []
    start = None
    end = None
    for item in collection.get_items():
        id_list.append(item.id)
        mean_list.append(item.properties["mean"])
        item_start_dt, item_end_dt = parse_item_time_range(item)
        if start == None or item_start_dt < start:
            start = item_start_dt
        if end == None or item_end_dt > end:
            end = item_end_dt


def main(collection_href: str) -> None:
    collection = Collection.from_file(collection_href)
    metrics = collect_collection_metrics(collection)
    calendar = Calendar(metrics.start, metrics.end, datetime.timedelta(hours=1))
    overall_rank = 1
    declustered_rank = 1
    year_rank_dict: dict[int, tuple[int, int]] = {}
    sorted_id_list = rank_ids(metrics.id_list, metrics.mean_list)
    for item_id in sorted_id_list:
        item = next(collection.get_items(item_id))
        item.properties["overall_rank"] = overall_rank
        overall_rank += 1
        item_start_dt, item_end_dt = parse_item_time_range(item)
        year_rank, declustered_year_rank = year_rank_dict.get(item_start_dt.year, (1, 1))
        item.properties["year_rank"] = year_rank
        year_rank += 1
        available = calendar.block_if_available(item_start_dt, item_end_dt)
        if available:
            item.properties["declustered_overall_rank"] = declustered_rank
            item.properties["declustered_year_rank"] = declustered_year_rank
            declustered_rank += 1
            declustered_year_rank += 1
        else:
            item.properties["declustered_rank"] = -1
            item.properties["declustered_year_rank"] = -1
        year_rank_dict[item_start_dt.year] = (year_rank, declustered_year_rank)
    collection.normalize_and_save(collection.get_self_href(), CatalogType.ABSOLUTE_PUBLISHED)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("collection_href", type=str)
    parser.add_argument("--duration_hours", type=int, default=72)

    args = parser.parse_args()

    main(args.collection_href, args.duration_hours)
