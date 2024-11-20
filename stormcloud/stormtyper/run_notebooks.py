import papermill as pm
from datetime import datetime
import json
from dask import delayed, compute
from dask.diagnostics import ProgressBar
from multiprocessing.pool import ThreadPool
from dask import config


def read_json(json_str):
    with open(json_str, "r") as file:
        dates = json.load(file)
    return dates


@delayed
def execute_single_notebook(input_notebook_path, output_folder, date):
    """
    Execute a single notebook for a given date and save the output in a specified folder
    """
    print(f"Processing date: {date}")

    # Define storm parameters
    storm_params = {
        "vars_2d": {
            "SBCAPE": 6,
            "PREC_ACC_NC": 1,
            "PWAT": 6,
            "PSFC": 6,
            "SRH03": 6,
            "Z_50000Pa": 12,
            "IVT": 6,
        },
        "precip_accum_interval": 6,
        "start_date_str": date,
        "duration_hours": 72,
    }

    # Format the output date string
    date_obj = datetime.strptime(date, "%Y/%m/%d-%HZ")
    output_date = date_obj.strftime("%Y_%m_%d")

    # Execute the notebook
    output_notebook = f"{output_folder}/storm-{output_date}.ipynb"
    pm.execute_notebook(
        input_notebook_path,
        output_notebook,
        parameters={"storm_params": storm_params},
    )
    return f"Completed: {output_notebook}"


def execute_storm_notebooks(input_notebook_path, output_folder, storm_dates):
    """
    Execute a given notebook for a series of storm dates in parallel
    """
    tasks = [
        execute_single_notebook(input_notebook_path, output_folder, date)
        for date in storm_dates
    ]

    # Configure the number of workers
    with config.set(scheduler="threads", pool=ThreadPool(2)):
        with ProgressBar():
            results = compute(*tasks)
    print("\n".join(results))


if __name__ == "__main__":
    input_json_str = "duwamish_final_typing_dates.json"
    output_notebooks_location = "notebooks"
    input_notebook = "stormtyper.ipynb"

    storm_dates = read_json(input_json_str)
    storm_dates1 = storm_dates[0:2]

    execute_storm_notebooks(input_notebook, output_notebooks_location, storm_dates1)
