import papermill as pm
from datetime import datetime
import json


def read_json(json_str):
    with open(json_str, "r") as file:
        dates = json.load(file)
    return dates


def execute_storm_notebooks(input_notebook_path, output_folder, storm_dates):
    """
    Execute a given notebook for a series of storm dates and save the outputs in a specified output folder
    """
    for date in storm_dates:
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


storm_dates = read_json("input_dates.json")
execute_storm_notebooks("stormtyper.ipynb", "notebooks", storm_dates)
