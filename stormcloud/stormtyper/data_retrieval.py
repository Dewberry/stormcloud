import io
import logging
from datetime import datetime, timedelta
from typing import List, Callable, Dict, Any
import os
import json
import boto3
import pandas as pd
import matplotlib.pyplot as plt
import xarray as xr
from dotenv import find_dotenv, load_dotenv
from IPython.display import HTML
from PIL import Image

from constants import (
    SOUTH_NORTH_DIM,
    SOUTH_NORTH_DIM_3D,
    TIME_DIM,
    URL_ROOT,
    WEST_EAST_DIM,
    WEST_EAST_DIM_3D,
    TRINITY_IBTRACS_JSON,
)
from conus404_utils import calc_geopot_at_press_lvl, calculate_slp, destagger_grid

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


def process_date(date_str: str):
    """
    Process a date string and return the year, month, day, hour, and water year.
    """
    date_obj = datetime.strptime(date_str, "%Y-%m-%d_%H")
    year = date_obj.year
    month = f"{date_obj.month:02d}"
    day = f"{date_obj.day:02d}"
    hour = f"{date_obj.hour:02d}"
    water_year = year + 1 if date_obj.month > 9 else year

    return year, month, day, hour, water_year


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


def get_2d_dataset(variable: str, date: str) -> xr.Dataset:
    """Fetch basic 2D variable datasets, derive sea level pressure (SLP) if needed"""

    # Extract date vars for URL
    year, month, day, hour, water_year = process_date(date)

    url_var_format = f"{variable}{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"

    # Construct the dataset URL for the 2D variables
    url_2d = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},{url_var_format}"
    ds_2d = xr.open_dataset(url_2d)

    # If the variable is surface pressure (PSFC), terrain height must be combined with PSFC to create more useful sea level pressure(SLP)
    if variable == "PSFC":
        # URL for terrain height data
        constants_url = f"{URL_ROOT}/INVARIANT/wrfconstants_usgs404.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},HGT[0:1:0]{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
        ds_constants = xr.open_dataset(constants_url)
        ds_2d["SLP"] = calculate_slp(
            ds_2d["PSFC"][0, :, :] * 0.01, ds_constants["HGT"][0, :, :]
        )
        ds_2d["SLP"] = ds_2d["SLP"].expand_dims(dim={"Time": ds_2d["Time"]})
        ds_2d = ds_2d.drop_vars(["PSFC"])

    return ds_2d


def get_3d_dataset(z_var: str, date: str) -> xr.Dataset:
    """
    Fetch and derive data for specified geopotential height variable.
    """

    # Extract date vars for URL
    year, month, day, hour, water_year = process_date(date)

    url_3d = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf3d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},XLONG{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},P[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},Z[0:1:0][0:1:50]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D}"
    ds_3d = xr.open_dataset(url_3d)
    # Destagger geopotential height grid
    ds_3d["Z_unstag"] = destagger_grid(ds_3d["Z"])

    # Calculate geopotential height at pressure level from z_var
    pressure_level = z_var.lstrip("Z_").rstrip("Pa")
    ds_3d[z_var] = calc_geopot_at_press_lvl(
        ds_3d["P"], ds_3d["Z_unstag"], pressure_level
    )
    ds = ds_3d.drop_vars(["P", "Z", "Z_unstag"])
    return ds


import xarray as xr
from datetime import datetime
from typing import List


import xarray as xr
from datetime import datetime
from typing import List


def get_precip_dataset(
    dates: List[str], precip_accum_interval: int
) -> (List[tuple], List[tuple]):
    """
    Fetch and accumulate precipitation datasets over a given interval.
    Returns two lists of datasets:
    1. Cumulative list: Each dataset is a sum of the current and the last cumulative dataset.
    2. Simple list: Each dataset is added as it is, without summation.
    Each dataset in both lists is paired with its associated end date.
    """
    prec_acc_nc_data = []
    cumulative_datasets = []  # List to hold the tuples of cumulative datasets and dates
    rolling_datasets = []  # List to hold the tuples of simple datasets and dates

    for date in dates:
        # Extract date vars for URL
        year, month, day, hour, water_year = process_date(date)

        # URL with just the accumulated precip variable
        precip_url = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},PREC_ACC_NC{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
        ds = xr.open_dataset(precip_url)

        if "PREC_ACC_NC" in ds.data_vars:
            # Append the 'PREC_ACC_NC' data array to the list
            prec_acc_nc_data.append(ds["PREC_ACC_NC"])

            if len(prec_acc_nc_data) == precip_accum_interval:
                # Concatenate along the time dimension
                accumulated_prec = xr.concat(prec_acc_nc_data, dim="Time").sum(
                    dim="Time"
                )
                new_dataset = accumulated_prec.to_dataset()

                # Format the end time for this dataset
                end_time = datetime.strptime(date, "%Y-%m-%d_%H")

                # For cumulative list, add the last dataset if it exists
                if cumulative_datasets:
                    new_cumulative_dataset = new_dataset + cumulative_datasets[-1][0]
                    cumulative_datasets.append((new_cumulative_dataset, end_time))
                else:
                    cumulative_datasets.append((new_dataset, end_time))

                # For simple list, just add the new dataset
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
):
    """
    Gather data and generate GIF for accumulated precip variable.
    """
    param_plots = []
    if var in vars_dates:
        dates = vars_dates[var]
        first_date = dates[0]  # used for gif output naming
        cumulative_datasets, rolling_datasets = data_getter(
            dates, storm_params["precip_accum_interval"]
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
    data_getter: Callable,
    plotter: Callable,
    alt_var: str = None,
):
    """
    Gather data and generate GIF for alternative variable.
    """
    param_plots = []
    if var in vars_dates:
        dates = vars_dates[var]
        first_date = dates[0]  # used for gif output naming
        for date in dates:
            ds = data_getter(var, date)

            if alt_var:
                plotter(ds, alt_var)
            else:
                plotter(ds, var)

            fig = plt.gcf()
            plt.close(fig)
            param_plots.append(fig)

    # Create GIF from matplotlib plots
    gif_filename = create_gif(param_plots, first_date, var)

    # Display the GIF in the notebook
    return HTML(f'<img src="{gif_filename}" />')
