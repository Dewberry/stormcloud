import datetime

import numpy as np
from pystac import Catalog, CatalogType, Item


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


def parse_item_time_range(item: Item) -> tuple[datetime.datetime, datetime.datetime]:
    item_start_dt = datetime.datetime.strptime(item.properties["start_datetime"], "%Y-%m-%dT%H:%M:%SZ")
    item_end_dt = datetime.datetime.strptime(item.properties["end_datetime"], "%Y-%m-%dT%H:%M:%SZ")
    if item_start_dt == item_end_dt:
        raise ValueError(f"Item {item.id} start ({item_start_dt}) and item end ({item_end_dt}) are the same")
    return item_start_dt, item_end_dt


def rank_catalog(catalog_href: str, duration_hours: int) -> None:
    catalog = Catalog.from_file(catalog_href)
    id_list: list[str] = []
    mean_list: list[float] = []
    start = None
    end = None
    for item in catalog.get_items():
        id_list.append(item.id)
        mean_list.append(item.properties["mean"])
        item_start_dt, item_end_dt = parse_item_time_range(item)
        if start == None or item_start_dt < start:
            start = item_start_dt
        if end == None or item_end_dt > end:
            end = item_end_dt
    calendar = Calendar(start, end, datetime.timedelta(hours=1))
    id_array = np.array(id_list, dtype=np.dtypes.StrDType)
    mean_array = np.array(mean_list, dtype=np.float64)
    sorted_indexes = np.argsort(mean_array)[::-1]
    sorted_id_array = id_array[sorted_indexes]
    true_rank = 1
    declustered_rank = 1
    for item_id in sorted_id_array.tolist():
        item = next(catalog.get_items(item_id))
        item.properties["rank"] = true_rank
        true_rank += 1
        unblocked = calendar.unblocked_dates()
        item_start_dt, item_end_dt = parse_item_time_range(item)
        np_item_start_dt = np.datetime64(item_start_dt)
        np_item_end_dt = np.datetime64(item_end_dt)
        if np_item_start_dt in unblocked and np_item_end_dt in unblocked:
            calendar.block_window(np_item_start_dt, np_item_end_dt)
            item.properties["declustered_rank"] = declustered_rank
            declustered_rank += 1
        else:
            item.properties["declustered_rank"] = -1
    catalog.normalize_and_save(catalog.get_self_href(), CatalogType.ABSOLUTE_PUBLISHED)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("catalog_href", type=str)
    parser.add_argument("--duration_hours", type=int, default=72)

    args = parser.parse_args()

    rank_catalog(args.catalog_href, args.duration_hours)
