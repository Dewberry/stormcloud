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

    data_type = "precipitation"

    # convert str to datetime
    start = datetime.strptime(start, "%Y-%m-%d")

    # read in watershed geometry and transposition domain geometry (shapely polygons)
    transposition_geom = None
    watershed_geom = None
    minimum_threshold = 1  # read in with geometries

    # read AORC data into xarray (time series)
    # this will be used later to write to dss
    try:
        xdata = get_xr_dataset(data_type, start, duration, mask=transposition_geom)
        logging.info(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {
                        "data_type": data_type,
                        "start": start.strftime("%Y-%m-%d"),
                        "duration": duration,
                        "aggregate_method": "",
                        "mask": "",
                    },  # should have some identifier for the transposition geom (mask)
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "failed",
                    "params": {
                        "data_type": data_type,
                        "start": start.strftime("%Y-%m-%d"),
                        "duration": duration,
                        "aggregate_method": "",
                        "mask": "",
                    },  # should have some identifier for the transposition geom (mask)
                    "error": str(e),
                }
            )
        )

    # read AORC data into xarray (aggregate)
    # this will be used for clustering/identifying storms
    aggregate_method = "sum"
    try:
        xsum = get_xr_dataset(data_type, start, duration, aggregate_method=aggregate_method, mask=transposition_geom)
        logging.info(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {
                        "data_type": data_type,
                        "start": start.strftime("%Y-%m-%d"),
                        "duration": duration,
                        "aggregate_method": aggregate_method,
                        "mask": "",
                    },  # should have some identifier for the transposition geom (mask)
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "job": get_xr_dataset.__name__,
                    "status": "failed",
                    "params": {
                        "data_type": data_type,
                        "start": start.strftime("%Y-%m-%d"),
                        "duration": duration,
                        "aggregate_method": aggregate_method,
                        "mask": "",
                    },  # should have some identifier for the transposition geom (mask)
                    "error": str(e),
                }
            )
        )

    # determine target number of cells (function)
    target_n_cells = 1000

    # get precipitation numpy array
    data = xsum.APCP_surface.to_numpy()

    # run clustering algorithm
    clusterer = Clusterer(data, target_n_cells, minimum_threshold)
    cluster_labels = clusterer.db_cluster()

    # iterate through clusters
    for cluster_id in np.unique(cluster_labels):
        cluster = clusterer.get_cluster(cluster_labels, cluster_id)

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
