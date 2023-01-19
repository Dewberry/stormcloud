from datetime import datetime
from itertools import product
import json
from logger import set_up_logger, log_to_json
import logging
from multiprocessing import Pool
import numpy as np
import sys
from storms.cluster import (
    Clusterer,
    Cluster,
    get_xr_dataset,
    number_of_cells,
    write_dss,
    adjust_cluster_size,
    rank_by_max,
    rank_by_mean,
    rank_by_norm,
)


def main(start: str, duration: int):
    """
    Main function to extract clusters from hourly AORC precipitation grids.
    AORC data is read and aggregated in an xarray dataset.
    A combination of thresholding and clustering is used to identify continguous clusters
    containing the greatest accumulated precipitation.

    Multiprocessing is used for cluster size adjustments where cells are iteratively added and
    removed from a cluster until the desired target number of cells are reached. In some instances,
    removing a cell can make a cluster non-contiguous (i.e., two separate clusters). In this case,
    those disconnected clusters are added back into the processing list (`args` variable). Additionally,
    any cluster that has obtained the desired number of cells is removed from the processing list.
    The process then restarts for any clusters remaining in the processing list, until that list is
    empty, meaning that all clusters are at the desired size.

    Once all clusters have finished processing, statistics and ranks are gathered to determine which
    cluster has the greatest average, maximum, and normalized average accumulate precipitation.


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

    # determine target number of cells
    target_n_cells = number_of_cells(xsum, watershed_geom)

    # get precipitation numpy array
    data = xsum.APCP_surface.to_numpy()

    # run clustering algorithm
    clusterer = Clusterer(data, target_n_cells, minimum_threshold)
    cluster_labels = clusterer.db_cluster()

    # adjust clusters' sizes (multi-processing)
    args = list(
        product([clusterer.get_cluster(cluster_labels, label) for label in np.unique(cluster_labels)], [target_n_cells])
    )

    # will hold the final clusters
    final_clusters = []

    while args:
        with Pool(4) as p:
            results = p.starmap(adjust_cluster_size, args)

        # flatten results (potentially mixed returns of Clusters and lists)
        results = [
            *[cluster for cluster in results if isinstance(cluster, Cluster)],
            *[cluster for split_clusters in results if isinstance(split_clusters, list) for cluster in split_clusters],
        ]

        # overwrite args with "unfinished" (split) clusters
        args = [cluster for cluster in results if cluster.size != target_n_cells]

        # add "finished" clusters to the final list
        final_clusters.extend([cluster for cluster in results if cluster.size == target_n_cells])

    # gather statistics on clusters (how to handle ties?)
    # mean_cluster = final_clusters[np.where(mean_ranks == 1)[0][0]]
    mean_ranks = rank_by_mean(final_clusters)
    mean_cluster = final_clusters[np.argmax(mean_ranks)]

    max_ranks = rank_by_max(final_clusters)
    max_cluster = final_clusters[np.argmax(max_ranks)]

    norm_ranks = rank_by_norm(final_clusters)
    norm_cluster = final_clusters[np.argmax(norm_ranks)]

    # store cluster data (png, nosql)

    # write grid to dss
    dss_file = ""  # placeholder
    aoi_name = ""  # placeholder

    write_dss(
        xdata,
        dss_file,
        "SHG4K",
        aoi_name.upper(),
        "PRECIPITATION",
        "AORC",
        resolution=4000,
    )


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
