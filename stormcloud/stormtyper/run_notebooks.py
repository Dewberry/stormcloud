import papermill as pm
from datetime import datetime

storm_dates = [
    "2017/08/28-00z",
    "2015/10/24-00z",
    "1989/05/14-00z",
    "1986/03/11-00z",
]


for date in storm_dates:
    print(date)
    storm_params = {
        "vars_2d": {
            "SBCAPE": 6,
            "PREC_ACC_NC": 1,
            "PWAT": 6,
            "PSFC": 6,
            "SRH03": 6,
        },
        "precip_accum_interval": 6,
        "start_date_str": date,
        "duration_hours": 24,
    }
    date_obj = datetime.strptime(date, "%Y/%m/%d-%HZ")
    output_date = date_obj.strftime("%Y_%m_%d")

    pm.execute_notebook(
        "stormtyper.ipynb",
        f"notebooks/storm-{output_date}.ipynb",
        parameters={"storm_params": storm_params},
    )
