import io
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List

import boto3
import matplotlib.pyplot as plt
import xarray as xr
from constants import (
    TRINITY_IBTRACS_JSON,
)
from dotenv import find_dotenv, load_dotenv
from IPython.display import HTML
from PIL import Image

load_dotenv(find_dotenv())
session = boto3.session.Session()
s3_client = session.client("s3")

logging.root.handlers = []
logging.basicConfig(
    level=logging.CRITICAL,
    format="""{"time": "%(asctime)s" , "level": "%(levelname)s", "message": "%(message)s"}""",
    handlers=[logging.StreamHandler()],
)


def process_date_intervals(
    start_date_str: str, duration_hours: int, vars_2d: Dict[str, int]
) -> Dict[str, List[str]]:
    """
    Generate a dictionary of date strings for different variables over specified duration intervals.
    """
    start_date = datetime.strptime(start_date_str, "%Y/%m/%d-%Hz")

    variable_dates = {var: [] for var in vars_2d}

    for var, interval in vars_2d.items():
        current_date = start_date
        end_date = start_date + timedelta(hours=duration_hours)
        while current_date <= end_date:
            # Since PREC_ACC_NC is accumulated precip over the past hour, add 1 hour to each time to get desired interval
            adjusted_date = (
                current_date + timedelta(hours=1)
                if var == "PREC_ACC_NC"
                else current_date
            )
            variable_dates[var].append(adjusted_date.strftime("%Y-%m-%d_%H"))
            current_date += timedelta(hours=interval)

    # Remove the last hour from PREC_ACC_NC dates for desired interval
    if "PREC_ACC_NC" in variable_dates:
        variable_dates["PREC_ACC_NC"] = variable_dates["PREC_ACC_NC"][:-1]

    # Sort dates for each variable
    for var in variable_dates:
        variable_dates[var].sort(key=lambda d: datetime.strptime(d, "%Y-%m-%d_%H"))

    return variable_dates


def get_widget_dates(start_date_str, duration_hours):
    start_date = datetime.strptime(start_date_str[:-2], "%Y/%m/%d-%H")

    # Calculate the end date by adding the duration in hours
    end_date = start_date + timedelta(hours=duration_hours)

    # Collect all full days within the range
    current_date = start_date
    full_days = []

    while current_date < end_date:
        full_days.append(current_date.strftime("%Y/%m/%d"))
        current_date += timedelta(days=1)
    return full_days


def search_tropical_events(start_date_str, duration):
    """
    Given a start date and a duration, finds tropical storm tracks within the time period.
    Uses data from JSON that has been filtered to just Trinity Basin.
    """
    with open(TRINITY_IBTRACS_JSON, "r") as f:
        tropical_storms_data = json.load(f)
    start_date = datetime.strptime(start_date_str, "%Y/%m/%d-%HZ")
    date_list = [start_date + timedelta(hours=i) for i in range(duration + 1)]
    date_list = list(set([date.date() for date in date_list]))
    storm_data = {}
    for event in tropical_storms_data:
        event_start_date = datetime.strptime(event["start"], "%Y-%m-%d").date()
        event_end_date = datetime.strptime(event["end"], "%Y-%m-%d").date()

        # Generate a list of dates within the event's duration
        event_date_list = [
            event_start_date + timedelta(days=i)
            for i in range((event_end_date - event_start_date).days + 1)
        ]

        # Check for any overlap between date_list and event_date_list
        if any(date in event_date_list for date in date_list):
            storm_data[event["name"]] = {
                "start_date": event_start_date.strftime("%Y-%m-%d"),
                "end_date": event_end_date.strftime("%Y-%m-%d"),
            }

    return storm_data


def get_precip_dataset(
    dates: List[str], precip_accum_interval: int, zarr_path: str
) -> (List[tuple], List[tuple]):
    """
    Fetch and accumulate precipitation datasets over a given interval.
    Returns two lists of datasets:
    1. cumulative_datasets: Each dataset is a sum of the current and the last cumulative dataset. Creates total accumulation over the entire time period.
    2. rolling_datasets: Each dataset is added as is for a given interval. Creates rolling accumulations that reset at given interval.
    Each dataset in both lists is paired with its associated end date.
    """
    prec_acc_nc_data = []
    cumulative_datasets = []  # List to store total accumulations datasets
    rolling_datasets = []  # List for rolling accumulation datasets
    first_date = dates[0].rsplit("_", 1)[0] + "_00"
    ds = xr.open_zarr(zarr_path, group=first_date)

    for date in dates:
        # Extract the target time
        target_time = datetime.strptime(date, "%Y-%m-%d_%H")

        # Select the corresponding dataset for the target time
        if "PREC_ACC_NC" in ds.data_vars:
            selected_data = ds["PREC_ACC_NC"].sel(time=target_time)

            # Append the selected data array to the list
            prec_acc_nc_data.append(selected_data)

            if len(prec_acc_nc_data) == precip_accum_interval:
                # Concatenate along the time dimension
                accumulated_prec = xr.concat(prec_acc_nc_data, dim="time").sum(
                    dim="time"
                )
                new_dataset = accumulated_prec.to_dataset(name="PREC_ACC_NC")

                # Format the end time for this dataset
                end_time = target_time

                # For cumulative list, add the last dataset if it exists
                if cumulative_datasets:
                    new_cumulative_dataset = new_dataset + cumulative_datasets[-1][0]
                    cumulative_datasets.append((new_cumulative_dataset, end_time))
                else:
                    cumulative_datasets.append((new_dataset, end_time))

                # For rolling list, just add the new dataset
                rolling_datasets.append((new_dataset, end_time))

                # Reset the list for the next batch
                prec_acc_nc_data = []

    return cumulative_datasets, rolling_datasets


def create_gif(
    param_plots: List[plt.Figure], timestamp: str, var_name: str, gif_folder="gifs"
) -> str:
    """
    Create an animated GIF from a list of matplotlib plots.
    """
    if not os.path.exists(f"notebooks/{gif_folder}"):
        os.makedirs(f"notebooks/{gif_folder}")

    # Convert figures to PIL Images
    images = []
    total_plots = len(param_plots)
    for idx, fig in enumerate(param_plots, start=1):
        # Add slide number to each plot
        fig.text(0.15, 0.90, f"{idx}/{total_plots}", fontsize=12, ha="right", va="top")
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        image = Image.open(buf)
        images.append(image)

    # Duplicate last image to make it last longer before restarting GIF
    last_image = images[-1].copy()
    images.append(last_image)

    # Save PIL images as GIF
    gif_filename = f"notebooks/{gif_folder}/{var_name}_animation_{timestamp}.gif"
    gif_location = f"{gif_folder}/{var_name}_animation_{timestamp}.gif"
    images[0].save(
        gif_filename,
        save_all=True,
        append_images=images[1:],
        duration=1200,  # Duration in ms per frame
        loop=0,
    )
    return gif_location


def precip_plotter(
    vars_dates: Dict[str, List[str]],
    storm_params: Dict[str, Any],
    var: str,
    data_getter: Callable,
    plotter: Callable,
    zarr_path,
):
    """
    Gather data and generate GIF for accumulated precip variable.
    """
    param_plots = []
    if var in vars_dates:
        dates = vars_dates[var]
        first_date = dates[0]  # used for gif output naming
        cumulative_datasets, rolling_datasets = data_getter(
            dates, storm_params["precip_accum_interval"], zarr_path
        )

    for (cumulative_ds, cumulative_end_time), (
        rolling_ds,
        rolling_end_time,
    ) in zip(cumulative_datasets, rolling_datasets):
        plotter(
            rolling_ds,
            cumulative_ds,
            var,
            rolling_end_time,
            storm_params["precip_accum_interval"],
        )
        fig = plt.gcf()
        plt.close(fig)
        param_plots.append(fig)

    # Create GIF from matplotlib plots
    gif_filename = create_gif(param_plots, first_date, var)
    # Display the GIF in the notebook
    return HTML(f'<img src="{gif_filename}" />')


def alt_plotter(
    vars_dates: Dict[str, List[str]],
    var: str,
    plotter: Callable,
    zarr_path: str,
    alt_var: str = None,
):
    """
    Gather data and generate GIF for alternative variable.
    """
    param_plots = []
    if var in vars_dates:
        dates = vars_dates[var]
        first_date = dates[0]  # used for gif output naming
        ds = xr.open_zarr(zarr_path, group=first_date)
        for date in dates:
            target_time = datetime.strptime(date, "%Y-%m-%d_%H")
            selected_ds = ds.sel(time=target_time)

            if alt_var:
                plotter(selected_ds, alt_var)
            else:
                plotter(selected_ds, var)

            fig = plt.gcf()
            plt.close(fig)
            param_plots.append(fig)

    # Create GIF from matplotlib plots
    gif_filename = create_gif(param_plots, first_date, var)

    # Display the GIF in the notebook
    return HTML(f'<img src="{gif_filename}" />')
