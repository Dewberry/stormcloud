from datetime import datetime, timedelta
from typing import Any, Callable, Dict, List
import os
import json
import xarray as xr
import zarr
import time
import logging
from constants import (
    SOUTH_NORTH_DIM,
    SOUTH_NORTH_DIM_3D,
    TIME_DIM,
    URL_ROOT,
    WEST_EAST_DIM,
    WEST_EAST_DIM_3D,
)
from conus404_utils import (
    calc_geopot_at_press_lvl,
    calculate_slp,
    destagger,
    calculate_ivt,
)

logging.basicConfig(
    filename="failed_zarr_dates.log",
    level=logging.ERROR,
    format="%(asctime)s - %(message)s",
    filemode="w",
)


def read_dates(dates_json: str) -> List[str]:
    with open(dates_json, "r") as file:
        dates = json.load(file)

    formatted_dates = []
    for date_str in dates:
        dt = datetime.strptime(date_str, "%Y/%m/%d-%HZ")
        formatted_date = dt.strftime("%Y-%m-%d_%H")
        formatted_dates.append(formatted_date)

    return formatted_dates


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


def generate_date_list(
    start_date: str, duration_hours: int, interval_hours: int
) -> List[str]:
    """
    Generate a list of date strings in the format 'YYYY-MM-DD_HH' for a given duration in hours,
    with specified intervals.

    Args:
        start_date (str): The start date in the format 'YYYY-MM-DD_HH'.
        duration_hours (int): The total duration in hours.
        interval_hours (int): The interval in hours between consecutive dates.

    Returns:
        List[str]: A list of date strings at the specified interval up to the duration.
    """
    start_datetime = datetime.strptime(start_date, "%Y-%m-%d_%H")

    date_list = [
        (start_datetime + timedelta(hours=i)).strftime("%Y-%m-%d_%H")
        for i in range(0, duration_hours + 1, interval_hours)
    ]
    return date_list


def get_2d_dataset(variable: str, date: str) -> xr.Dataset:
    """Fetch basic 2D variable datasets, derive sea level pressure (SLP) if needed"""

    # Extract date vars for URL
    year, month, day, hour, water_year = process_date(date)

    url_var_format = f"{variable}{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"

    # Construct the dataset URL for the 2D variables
    url_2d = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},{url_var_format}"
    ds_2d = xr.open_dataset(url_2d, engine="netcdf4")

    # If the variable is surface pressure (PSFC), terrain height must be combined with PSFC to create sea level pressure(SLP)
    if variable == "PSFC":
        # URL for terrain height data
        constants_url = f"{URL_ROOT}/INVARIANT/wrfconstants_usgs404.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},HGT[0:1:0]{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
        ds_constants = xr.open_dataset(constants_url)
        ds_2d["SLP"] = calculate_slp(
            ds_2d["PSFC"][0, :, :] * 0.01, ds_constants["HGT"][0, :, :]
        )
        ds_2d["SLP"] = ds_2d["SLP"].expand_dims(dim={"Time": ds_2d["Time"]})
        ds_2d = ds_2d.drop_vars(["PSFC"])

    ds_2d = ds_2d.astype({var: "float16" for var in ds_2d.data_vars})

    return ds_2d


def get_3d_dataset(var_3d: str, date: str) -> xr.Dataset:
    """
    Fetch and derive data for specified geopotential height variable.
    """
    # Extract date vars for URL
    year, month, day, hour, water_year = process_date(date)

    if var_3d == "Z_50000Pa":
        url_z = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf3d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},XLONG{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},P[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},Z[0:1:0][0:1:50]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D}"
        ds_3d = xr.open_dataset(url_z, engine="netcdf4")

        # Destagger geopotential height grid
        unstaggered_Z = destagger(ds_3d["Z"])
        unstaggered_Z = unstaggered_Z.assign_coords(
            {"bottom_top": ds_3d["bottom_top_stag"][:-1]}
        )
        unstaggered_Z = unstaggered_Z.swap_dims({"bottom_top_stag": "bottom_top"})
        ds_3d["Z_unstag"] = unstaggered_Z

        # Calculate geopotential height at pressure level from z_var
        pressure_level = var_3d.lstrip("Z_").rstrip("Pa")
        ds_3d[var_3d] = calc_geopot_at_press_lvl(
            ds_3d["P"], ds_3d["Z_unstag"], pressure_level
        )
        ds = ds_3d.drop_vars(["P", "Z", "Z_unstag"])

    if var_3d == "IVT":

        url_ivt = f"{URL_ROOT}/wy{water_year}/{year}{month}/wrf3d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},XLONG{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},V[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D},U[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D}, P[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D}, QVAPOR[0:1:0][0:1:49]{SOUTH_NORTH_DIM_3D}{WEST_EAST_DIM_3D}"
        ds_3d = xr.open_dataset(url_ivt, engine="netcdf4")

        mixing_ratio = ds_3d["QVAPOR"]
        u_wind = ds_3d["U"]
        v_wind = ds_3d["V"]
        pressure = ds_3d["P"]
        # Rename staggered dimensions
        u_wind = u_wind.rename({"west_east_stag": "west_east"})
        v_wind = v_wind.rename({"south_north_stag": "south_north"})
        ds = calculate_ivt(u_wind, v_wind, pressure, mixing_ratio)
    return ds


def main(var, dates, zarr_path, duration, interval):
    """
    Main processing function to fetch, process, and save data to Zarr,
    retrying twice for each date group if errors occur.
    """

    # Ensure Zarr path exists or create an empty group
    if not os.path.exists(zarr_path):
        zarr.open_group(zarr_path, mode="w")
        print(f"Empty Zarr file created at {zarr_path}")

    for date in dates:
        retries = 0
        max_retries = 2

        while retries <= max_retries:
            try:
                print(f"Processing {date}")
                date_list = generate_date_list(date, duration, interval)

                if not os.path.exists(zarr_path):
                    os.makedirs(zarr_path)

                # Use the first date in the list as the group name
                group_name = date_list[0].replace(":", "-")

                # Check if group (date) already exists
                store = zarr.open_group(zarr_path, mode="r")
                if group_name in store:
                    print(f"Group '{group_name}' already exists in Zarr. Skipping...")
                    break  # Skip this date and exit retry loop

                # Collect datasets for the group
                datasets = []
                for date_hour in date_list:
                    print(f"Processing {var} for {date_hour}")
                    if var == "Z_50000Pa" or var == "IVT":
                        ds = get_3d_dataset(var, date_hour)
                    else:
                        ds = get_2d_dataset(var, date_hour)

                    if "Time" in ds.dims:
                        ds = ds.rename({"Time": "time"})
                    datasets.append(ds)

                # Combine datasets and save to Zarr
                if datasets:
                    combined_ds = xr.concat(datasets, dim="time")
                    combined_ds.to_zarr(zarr_path, mode="a", group=group_name)
                    print(f"Saved {var} data to Zarr group {group_name} at {zarr_path}")
                else:
                    print(
                        f"No datasets were processed for {var} in the given date range."
                    )
                break  # Exit retry loop if successful

            except Exception as e:
                retries += 1
                print(
                    f"Failed to process {date} (attempt {retries}/{max_retries}): {e}"
                )
                logging.error(f"Error processing {date}: {e}")

                if retries > max_retries:
                    print(f"Skipping {date} after {max_retries} failed attempts.")
                    logging.error(
                        f"Skipping {date} after {max_retries} failed attempts."
                    )
                    break

                time.sleep(5)


if __name__ == "__main__":

    # vars = ["PREC_ACC_NC", "PSFC", "Z_50000Pa", "PWAT", "SRH03", "SBCAPE", "IVT"]
    json_str = "duwamish_final_typing_dates.json"
    duration = 72
    interval = 3
    var = "PWAT"
    zarr_path = f"Duwamish_{var}.zarr"

    dates = read_dates(json_str)
    dates1 = dates[0:3]
    main(var, dates1, zarr_path, duration, interval)
