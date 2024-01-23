import io
import logging
from datetime import datetime, timedelta
from typing import List, Callable, Dict, Any

import boto3
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


def get_precip_dataset(
    dates: List[str], precip_accum_interval: int
) -> List[xr.Dataset]:
    """
    Fetch and accumulate precipitation datasets over a given interval.
    """
    prec_acc_nc_data = []
    datasets = []  # List to hold the datasets

    for date in dates:
        # Extract date vars for URL
        year, month, day, hour, water_year = process_date(date)

        # URL with just the accumulated precip variable
        precip_url = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},PREC_ACC_NC{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
        ds = xr.open_dataset(precip_url)

        if "PREC_ACC_NC" in ds.data_vars:
            # Append the 'PREC_ACC_NC' data array to the list
            prec_acc_nc_data.append(ds["PREC_ACC_NC"])
            # Once length of data arrays list gets to desired interval, sum them
            if len(prec_acc_nc_data) == precip_accum_interval:
                # Concatenate along the time dimension and add to datasets list
                accumulated_prec = (
                    xr.concat(prec_acc_nc_data, dim="Time").sum(dim="Time").to_dataset()
                )
                # Append end_time with datasets for output plot titles
                end_time = datetime.strptime(date, "%Y-%m-%d_%H")
                datasets.append((accumulated_prec, end_time))
                prec_acc_nc_data = []  # Reset the list for the next batch

    return datasets


def create_gif(param_plots: List[plt.Figure], timestamp: str, var_name: str) -> str:
    """
    Create an animated GIF from a list of matplotlib plots.
    """
    # Convert figures to PIL Images
    images = []
    for fig in param_plots:
        buf = io.BytesIO()
        fig.savefig(buf, format="png")
        buf.seek(0)
        image = Image.open(buf)
        images.append(image)

    # Duplicate last image to make it last longer before restarting GIF
    last_image = images[-1].copy()
    images.append(last_image)

    # Save PIL images as GIF
    gif_filename = f"{var_name}_animation_{timestamp}.gif"
    images[0].save(
        gif_filename,
        save_all=True,
        append_images=images[1:],
        duration=1200,  # Duration in ms per frame
        loop=0,
    )
    return gif_filename


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
        datasets = data_getter(dates, storm_params["precip_accum_interval"])

    for ds, end_time in datasets:
        plotter(ds, var, end_time, storm_params["precip_accum_interval"])
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
