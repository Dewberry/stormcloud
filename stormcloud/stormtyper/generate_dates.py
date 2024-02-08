import json
from datetime import datetime
import random
from constants import RANKED_EVENTS_JSON


def get_top_event_dates(top_events_num=20):
    """Selects top events per year from json"""

    with open(RANKED_EVENTS_JSON, "r") as f:
        ranked_events = json.load(f)
    dates = []
    for event in ranked_events:
        if event["ranks"]["true_rank"] <= top_events_num:
            # Extract date portion from "id" field
            date_from_id = event["id"].split("_")[-1]
            date = datetime.strptime(date_from_id, "%Y%m%d").date()
            date_str = date.strftime("%Y/%m/%d")
            dates.append(date_str)

    return dates


def filter_dates_by_window(dates, start_date="1979/10/01", end_date="2022/09/30"):
    """Filtered dates so that they are within a given range"""

    start_date_obj = datetime.strptime(start_date, "%Y/%m/%d")
    end_date_obj = datetime.strptime(end_date, "%Y/%m/%d")
    filtered_dates = []
    for date in dates:
        if (
            datetime.strptime(date, "%Y/%m/%d") >= start_date_obj
            and datetime.strptime(date, "%Y/%m/%d") <= end_date_obj
        ):
            filtered_dates.append(date)

    return filtered_dates


def random_select_dates(dates, num_dates=200):
    """Randomly selects a given number of dates from a list of dates"""

    if num_dates > len(dates):
        raise ValueError(
            "Number of dates to select cannot be greater than the total number of dates."
        )
    return random.sample(dates, num_dates)


def add_time_to_dates(dates, time_str="-00z"):
    return [date + time_str for date in dates]


def convert_dates_to_json(dates, output_path="input_dates.json"):
    with open(output_path, "w") as file:
        json.dump(dates, file, indent=4)


if __name__ == "__main__":
    ranked_dates = get_top_event_dates()
    filtered_dates = filter_dates_by_window(ranked_dates)
    random_dates = random_select_dates(filtered_dates)
    final_dates = add_time_to_dates(random_dates)
    convert_dates_to_json(final_dates)
