from datetime import datetime
import json
import logging
import numpy as np
import sys
from storms.cluster import Clusterer, Cluster, get_xr_dataset
from logger import set_up_logger, log_to_json


def main(start: str, duration: int):
    """
    Main function to extract clusters from hourly AORC precipitation grids.

    Parameters
    start: string (%Y-%m-%d)
    duration: int (3) interval or duration of storms in hours

    example usage: python extract_storms.py 1979-02-01 2
    """

    # example
    ndarray = np.ndarray(shape=(2, 3))
    cells = 3
    minimum_threshold = 5

    c = Clusterer(ndarray, cells, minimum_threshold)

    # m = {"start": start, "duration": duration}
    logging.info(json.dumps({"function_call": c.__repr__}))

    # read AORC data into xarray
    try:
        data = get_xr_dataset()
        logging.info(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {"ndarray": ndarray, "cells": cells, "minimum_threshold": minimum_threshold},
                }
            )
        )
    except TypeError as e:
        logging.debug(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {"ndarray": ndarray.shape, "cells": cells, "minimum_threshold": minimum_threshold},
                }
            )
        )
        logging.error(json.dumps({get_xr_dataset.__name__: str(e)}))

    # get precipitation numpy array

    # determine target number of cells and the minimum threshold

    # run clustering algorithm

    # manipulate clusters to match target number of cells

    # gather statistics on clusters

    # store cluster data (png, nosql)

    # write grid to dss
    pass


if __name__ == "__main__":

    execution_time = datetime.now().strftime("%Y%m%d_%H%M")
    logfile = f"extract-storms-{execution_time}.log"

    # logger = set_up_logger(filename=logfile)
    logger = set_up_logger()
    logger.setLevel(logging.DEBUG)

    args = sys.argv

    start = args[1]
    duration = args[2]
    main(start, duration)

    # log_to_json(logfile)
