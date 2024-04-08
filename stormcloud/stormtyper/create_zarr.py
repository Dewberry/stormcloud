import json
from datetime import datetime, timedelta

import xarray as xr
from notebooks.constants import SOUTH_NORTH_DIM, TIME_DIM, URL_ROOT, WEST_EAST_DIM
from notebooks.conus404_utils import calculate_slp


def process_date(date_obj):
    year = date_obj.year
    month = f"{date_obj.month:02d}"
    day = f"{date_obj.day:02d}"
    hour = f"{date_obj.hour:02d}"
    water_year = year + 1 if date_obj.month > 9 else year

    return year, month, day, hour, water_year


def process_cape_datasets(dates, failed_dates, hour_of_highest_cape, total_highest_cape, duration=72):
    for date in dates:
        try:
            print(f"Processing {date}")
            date_obj = datetime.strptime(date, "%Y_%m_%d")
            # Create empty list to store dataset for each hour (72 hour in our case)
            sbcape_data = []
            # i starts at 0 and ends at 71
            for i in range(duration):
                # with every loop, add one hour to our start date and get the accumulated precip for that hour
                new_date = date_obj + timedelta(hours=i)
                year, month, day, hour, water_year = process_date(new_date)

                psfc_url = f"https://thredds.rda.ucar.edu/thredds/dodsC/files/g/ds559.0/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},SBCAPE{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
                ds = xr.open_dataset(psfc_url)

                # add the current hours dataset to the list
                sbcape_data.append(ds["SBCAPE"])
            # Calculate the grid with the highest value for each array position out of all arrays
            combined = xr.concat(sbcape_data, dim="Time")
            total_max_array = combined.max(dim="Time")
            total_highest_cape[date] = total_max_array
            # Find the array that has the highest cape value out of all arrays
            highest_max_value = None
            array_with_highest_max = None

            for da in sbcape_data:
                max_sbcape = da.max().item()

                if highest_max_value is None or max_sbcape > highest_max_value:
                    highest_max_value = max_sbcape
                    # Remove the "Time" dimension and coordinate
                    array_with_highest_max = da.squeeze(dim="Time").reset_coords("Time", drop=True)
            # Add the hour with the highest cape value to hour_of_highest_cape
            hour_of_highest_cape[date] = array_with_highest_max
            print(f"Successfully processed date: {date}")
        except Exception as e:
            # Add any failed dates to a list for easy tracking
            print(f"Failed to process {date}: {e}")
            failed_dates.append(date)


def process_slp_dataset(dates, failed_dates, hour_of_lowest_press, total_lowest_press, duration=72):
    for date in dates:
        try:
            print(f"Processing {date}")
            date_obj = datetime.strptime(date, "%Y_%m_%d")
            # Create empty list to store dataset for each hour (72 hour in our case)
            psfc_data = []
            # i starts at 0 and ends at 71
            for i in range(duration):
                # with every loop, add one hour to our start date and get the accumulated precip for that hour
                new_date = date_obj + timedelta(hours=i)
                year, month, day, hour, water_year = process_date(new_date)

                psfc_url = f"https://thredds.rda.ucar.edu/thredds/dodsC/files/g/ds559.0/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},PSFC{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
                ds = xr.open_dataset(psfc_url)

                constants_url = f"{URL_ROOT}/INVARIANT/wrfconstants_usgs404.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},HGT[0:1:0]{SOUTH_NORTH_DIM}{WEST_EAST_DIM}"
                ds_constants = xr.open_dataset(constants_url)
                ds["SLP"] = calculate_slp(ds["PSFC"][0, :, :] * 0.01, ds_constants["HGT"][0, :, :])
                ds["SLP"] = ds["SLP"].expand_dims(dim={"Time": ds["Time"]})
                # add the current hours dataset to the list
                psfc_data.append(ds["SLP"])
            # Calculate the grid with the lowest value for each array position out of all arrays
            combined = xr.concat(psfc_data, dim="Time")
            total_min_array = combined.min(dim="Time")
            total_lowest_press[date] = total_min_array
            # Find the array that has the lowest pressure value out of all arrays
            lowest_min_value = None
            array_with_lowest_min = None

            for da in psfc_data:
                min_psfc = da.min().item()

                if lowest_min_value is None or min_psfc < lowest_min_value:
                    lowest_min_value = min_psfc
                    array_with_lowest_min = da.squeeze(dim="Time").reset_coords("Time", drop=True)
            # Add each dates 72hr accumulation to the larger all_dates_ds dataset
            hour_of_lowest_press[date] = array_with_lowest_min
        except Exception as e:
            # Add any failed dates to a list for easy tracking
            print(f"Failed to process {date}: {e}")
            failed_dates.append(date)


def main(var, dates, hourly_output_fn, total_output_fun):

    if var == "SBCAPE":
        failed_dates = []

        hour_of_highest_cape = xr.Dataset()
        total_highest_cape = xr.Dataset()

        process_cape_datasets(dates, failed_dates, hour_of_highest_cape, total_highest_cape)

        # Process failed dates again
        if failed_dates:
            print(f"Retrying failed dates: {failed_dates}")
            retry_dates = failed_dates.copy()
            failed_dates.clear()
            process_cape_datasets(retry_dates, failed_dates, hour_of_highest_cape, total_highest_cape)

        hour_of_highest_cape.to_zarr(hourly_output_fn, mode="a")
        total_highest_cape.to_zarr(total_output_fun, mode="a")
    elif var == "SLP":

        failed_dates = []

        hour_of_lowest_press = xr.Dataset()
        total_lowest_press = xr.Dataset()

        process_slp_dataset(dates, failed_dates, hour_of_lowest_press, total_lowest_press)

        if failed_dates:
            print(f"Retrying failed dates: {failed_dates}")
            retry_dates = failed_dates.copy()
            failed_dates.clear()
            process_slp_dataset(retry_dates, failed_dates, hour_of_lowest_press, total_lowest_press)

        hour_of_lowest_press.to_zarr(hourly_output_fn, mode="a")
        total_lowest_press.to_zarr(total_output_fun, mode="a")


if __name__ == "__main__":

    # Enter "SBCAPE" or "SLP" for var
    var = "SBCAPE"

    hourly_output_fn = "highest_cape_hr.zarr"
    total_output_fun = "total_highest_cape.zarr"

    json_file_path = "typed_dates.json"
    with open(json_file_path, "r") as file:
        dates = json.load(file)

    main(var, dates, hourly_output_fn, total_output_fun)
