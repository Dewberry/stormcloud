from boto3 import Session
from datetime import datetime
import json
from logger import set_up_logger, log_to_json
import logging
import os
from storms.utils import plotter
import sys
from storms.cluster import (
    get_xr_dataset,
    write_dss,
    s3_geometry_reader,
    get_atlas14,
)
from storms.transpose import Transposer

session = Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])


def main(
    start: str,
    duration: int,
    domain_name: str,
    domain_uri: str,
    watershed_uri: str,
    dss_dir: str,
    png_dir: str,
    scale_max: int,
):

    data_type = "precipitation"

    # convert str to datetime
    start = datetime.strptime(start, "%Y-%m-%d")
    start_as_str = start.strftime("%Y%m%d")  # for use in file naming

    # read in watershed geometry and transposition domain geometry (shapely polygons)
    transposition_geom = s3_geometry_reader(session, domain_uri)
    watershed_geom = s3_geometry_reader(session, watershed_uri)

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
                        "aggregate_method": None,
                        "mask": domain_uri,
                    },
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
                        "aggregate_method": None,
                        "mask": domain_uri,
                    },
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
                        "mask": domain_uri,
                    },
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
                        "mask": domain_uri,
                    },
                    "error": str(e),
                }
            )
        )

    # get atlas 14 data for normalizing (ADD CHECK THAT ATLAS14 DATA COVERS DOMAIN)
    if duration <= 24:
        atlas_14_uri = f"s3://tempest/transforms/atlas14/2yr{duration:02d}ha/2yr{duration:02d}ha.vrt"
    else:
        # add check here that duration divisible by 24
        atlas_14_uri = f"s3://tempest/transforms/atlas14/2yr{int(duration/24):02d}da/2yr{int(duration/24):02d}da.vrt"

    # xnorm = get_atlas14(atlas_14_uri, xsum.APCP_surface)
    # norm_arr = xnorm.to_numpy() * 25.4  # convert to mm
    norm_arr = None

    # transpose watershed around transposition domain
    transposer = Transposer(xsum, watershed_geom, normalized_data=norm_arr)

    # get translation with greatest mean (max used as tie breaker)
    # if still tied after max (arbitrarily select the 1st one)
    mean_ranks = transposer.ranks("mean", rank_method="min")
    max_ranks = transposer.ranks("max", rank_method="min")
    translates = transposer.transposes[mean_ranks == 1]

    if len(translates) > 1:
        # log that there are ties
        translates = translates[max_ranks[mean_ranks == 1] == max_ranks[mean_ranks == 1].min()]

        if len(translates) > 1:
            # log still tied
            pass

    best_translate = translates[0]
    translate_geom = best_translate.geom

    # store cluster data (png, nosql)

    # pngs - add mm to inch conversion
    plotter.cluster_plot(
        xsum,
        translate_geom,
        0,
        scale_max,
        "Accumulation (MM)",
        geom=[watershed_geom, transposer.valid_space_geom()],
        png=os.path.join(png_dir, f"{start_as_str}.png"),
    )

    # write grid to dss
    write_dss(
        xdata,
        os.path.join(dss_dir, f"{start_as_str}.dss"),
        "SHG4K",
        domain_name.upper(),
        "PRECIPITATION",
        "AORC",
        resolution=4000,
    )


if __name__ == "__main__":

    execution_time = datetime.now().strftime("%Y%m%d_%H%M")
    logfile = f"outputs/logs/extract-storms-{execution_time}.log"

    logger = set_up_logger(filename=logfile)
    # logger = set_up_logger()
    logger.setLevel(logging.INFO)

    args = sys.argv

    start = args[1]
    duration = args[2]
    domain_name = args[3]
    domain_uri = args[4]
    watershed_uri = args[5]
    minimum_threshold = args[6]
    dss_dir = args[7]
    png_dir = args[8]
    scale_max = args[9]

    main(
        start,
        duration,
        domain_name,
        domain_uri,
        watershed_uri,
        dss_dir,
        png_dir,
        scale_max,
    )

    # log_to_json(logfile)
