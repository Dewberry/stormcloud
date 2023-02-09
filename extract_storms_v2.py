from boto3 import Session
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
import json
from logger import set_up_logger, log_to_json
import logging
import os
from storms.utils import plotter, ms
import sys
from storms.cluster import (
    get_xr_dataset,
    write_dss,
    s3_geometry_reader,
    get_atlas14,
)
from storms.transpose import Transposer

load_dotenv(find_dotenv())

session = Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
ms_client = ms.Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])


def main(
    start: str,
    duration: int,
    watershed_name: str,
    domain_version: str,
    domain_uri: str,
    watershed_uri: str,
    dss_dir: str,
    png_dir: str,
    scale_max: int,
    index_name: str,
):

    data_type = "precipitation"

    # convert str to datetime
    start = datetime.strptime(start, "%Y-%m-%d")
    start_as_str = start.strftime("%Y%m%d")  # for use in file naming

    # read in watershed geometry and transposition domain geometry (shapely polygons)
    try:
        transposition_geom = s3_geometry_reader(session, domain_uri)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": s3_geometry_reader.__name__,
                    "status": "success",
                    "params": {
                        "session": str(session),
                        "uri": domain_uri,
                        "layer": None,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": s3_geometry_reader.__name__,
                    "status": "failed",
                    "params": {
                        "session": str(session),
                        "uri": domain_uri,
                        "layer": None,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    try:
        watershed_geom = s3_geometry_reader(session, watershed_uri)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": s3_geometry_reader.__name__,
                    "status": "success",
                    "params": {
                        "session": str(session),
                        "uri": watershed_uri,
                        "layer": None,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": s3_geometry_reader.__name__,
                    "status": "failed",
                    "params": {
                        "session": str(session),
                        "uri": watershed_uri,
                        "layer": None,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # read AORC data into xarray (time series)
    # this will be used later to write to dss
    try:
        xdata = get_xr_dataset(data_type, start, duration, mask=transposition_geom)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
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
                    "event_date": start.strftime("%Y-%m-%d"),
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
        raise

    # read AORC data into xarray (aggregate)
    # this will be used for clustering/identifying storms
    aggregate_method = "sum"
    try:
        xsum = get_xr_dataset(data_type, start, duration, aggregate_method=aggregate_method, mask=transposition_geom)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
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
                    "event_date": start.strftime("%Y-%m-%d"),
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
        raise

    # get atlas 14 data for normalizing (ADD CHECK THAT ATLAS14 DATA COVERS DOMAIN)
    try:
        if duration <= 24:
            atlas_14_uri = f"s3://tempest/transforms/atlas14/2yr{duration:02d}ha/2yr{duration:02d}ha.vrt"
        else:
            # add check here that duration divisible by 24
            atlas_14_uri = (
                f"s3://tempest/transforms/atlas14/2yr{int(duration/24):02d}da/2yr{int(duration/24):02d}da.vrt"
            )

        # xnorm = get_atlas14(atlas_14_uri, xsum.APCP_surface)
        # norm_arr = xnorm.to_numpy() * 25.4  # convert to mm

        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": get_atlas14.__name__,
                    "status": "success",
                    "params": {
                        "s3_uri": atlas_14_uri,
                        "interpolate_to": "xsum",
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": get_atlas14.__name__,
                    "status": "failed",
                    "params": {
                        "s3_uri": atlas_14_uri,
                        "interpolate_to": "xsum",
                    },
                    "error": str(e),
                }
            )
        )
        raise

    norm_arr = None  # temporary for upper green processing

    # transpose watershed around transposition domain
    try:
        transposer = Transposer(xsum, watershed_geom, normalized_data=norm_arr)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": Transposer.__name__,
                    "status": "success",
                    "params": {
                        "xsum": "xsum",
                        "watershed_geom": watershed_uri,
                        "data_var": "APCP_surface",
                        "x_var": "longitude",
                        "y_var": "latitude",
                        "normalized_data": None,  # temporary for upper green processing
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": Transposer.__name__,
                    "status": "failed",
                    "params": {
                        "xsum": "xsum",
                        "watershed_geom": watershed_uri,
                        "data_var": "APCP_surface",
                        "x_var": "longitude",
                        "y_var": "latitude",
                        "normalized_data": None,  # temporary for upper green processing
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # get translation with greatest mean (max used as tie breaker)
    # if still tied after max (arbitrarily select the 1st one)

    # get mean ranks
    rank_method = "min"
    metric = "mean"
    try:
        mean_ranks = transposer.ranks("mean", rank_method)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.ranks.__name__,
                    "status": "success",
                    "params": {
                        "metric": metric,
                        "rank_method": rank_method,
                        "order_high_low": True,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.ranks.__name__,
                    "status": "failed",
                    "params": {
                        "metric": metric,
                        "rank_method": rank_method,
                        "order_high_low": True,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # get max ranks
    metric = "max"
    try:
        max_ranks = transposer.ranks(metric, rank_method)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.ranks.__name__,
                    "status": "success",
                    "params": {
                        "metric": metric,
                        "rank_method": rank_method,
                        "order_high_low": True,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.ranks.__name__,
                    "status": "failed",
                    "params": {
                        "metric": metric,
                        "rank_method": rank_method,
                        "order_high_low": True,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # select best translate

    try:
        translates = transposer.transposes[mean_ranks == 1]

        if len(translates) > 1:
            logging.warning(
                json.dumps(
                    {
                        "event_date": start.strftime("%Y-%m-%d"),
                        "job": "get_best_translate",
                        "message": f"{len(translates)} tied for mean",
                    }
                )
            )
            translates = translates[max_ranks[mean_ranks == 1] == max_ranks[mean_ranks == 1].min()]

            if len(translates) > 1:
                logging.warning(
                    json.dumps(
                        {
                            "event_date": start.strftime("%Y-%m-%d"),
                            "job": "get_best_translate",
                            "message": f"{len(translates)} tied for mean and max",
                        }
                    )
                )

        best_translate = translates[0]
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": "get_best_translate",
                    "status": "success",
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": "get_best_translate",
                    "status": "failed",
                    "error": str(e),
                }
            )
        )
        raise

    # get translate geom
    try:
        translate_geom = transposer.transpose_geom(best_translate)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.transpose_geom.__name__,
                    "status": "success",
                    "params": {"transpose": best_translate.to_dict()},
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": transposer.transpose_geom.__name__,
                    "status": "failed",
                    "params": {"transpose": best_translate.to_dict()},
                    "error": str(e),
                }
            )
        )
        raise

    # store cluster data (png, nosql)
    # pngs - add mm to inch conversion
    png_path = os.path.join(png_dir, f"{start_as_str}.png")
    scale_label = "Accumulation (MM)"
    scale_min = 0
    try:
        plotter.cluster_plot(
            xsum,
            translate_geom,
            scale_min,
            scale_max,
            scale_label,
            geom=[watershed_geom, transposer.valid_space_geom()],
            png=png_path,
        )

        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": plotter.cluster_plot.__name__,
                    "status": "success",
                    "params": {
                        "xdata": "xsum",
                        "cluster_geometry": "translate_geom",
                        "vmin": scale_min,
                        "vmax": scale_max,
                        "scale_label": scale_label,
                        "multiplier": 1,
                        "geom": ["watershed_geom", "transposer.valid_space_geom"],
                        "png": png_path,
                    },
                }
            )
        )

    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": plotter.cluster_plot.__name__,
                    "status": "failed",
                    "params": {
                        "xdata": "xsum",
                        "cluster_geometry": "translate_geom",
                        "vmin": scale_min,
                        "vmax": scale_max,
                        "scale_label": scale_label,
                        "multiplier": 1,
                        "geom": ["watershed_geom", "transposer.valid_space_geom"],
                        "png": png_path,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # write grid to dss
    dss_path = os.path.join(dss_dir, f"{start_as_str}.dss")
    path_a = "SHG4K"
    path_b = watershed_name.upper()
    path_c = "PRECIPITATION"
    path_f = "AORC"
    resolution = 4000
    try:
        write_dss(
            xdata,
            dss_path,
            path_a,
            path_b,
            path_c,
            path_f,
            resolution,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": write_dss.__name__,
                    "status": "success",
                    "params": {
                        "xdata": "xdata",
                        "dss_path": dss_path,
                        "path_a": path_a,
                        "path_b": path_b,
                        "path_c": path_c,
                        "path_f": path_f,
                        "resolution": resolution,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": write_dss.__name__,
                    "status": "failed",
                    "params": {
                        "xdata": "xdata",
                        "dss_path": dss_path,
                        "path_a": path_a,
                        "path_b": path_b,
                        "path_c": path_c,
                        "path_f": path_f,
                        "resolution": resolution,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    try:
        doc = ms.tranpose_to_doc(
            start, duration, watershed_name, domain_version, watershed_uri, domain_uri, best_translate
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": ms.tranpose_to_doc.__name__,
                    "status": "success",
                    "params": {
                        "event_start": str(start),
                        "duration": duration,
                        "watershed_name": watershed_name,
                        "domain_version": domain_version,
                        "watershed_uri": watershed_uri,
                        "domain_uri": domain_uri,
                        "transpose": best_translate.to_dict(),
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": ms.tranpose_to_doc.__name__,
                    "status": "failed",
                    "params": {
                        "event_start": str(start),
                        "duration": duration,
                        "watershed_name": watershed_name,
                        "domain_version": domain_version,
                        "watershed_uri": watershed_uri,
                        "domain_uri": domain_uri,
                        "transpose": best_translate.to_dict(),
                    },
                    "error": str(e),
                }
            )
        )
        raise

    try:
        ms.upload_doc(ms_client, index_name, doc)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": ms.upload_doc.__name__,
                    "status": "success",
                    "params": {
                        "client": "ms_client",
                        "index": index_name,
                        "doc": doc.to_dict(),
                    },
                }
            )
        )

    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": ms.upload_doc.__name__,
                    "status": "failed",
                    "params": {
                        "client": "ms_client",
                        "index": index_name,
                        "doc": doc.to_dict(),
                    },
                    "error": str(e),
                }
            )
        )
        raise


if __name__ == "__main__":

    execution_time = datetime.now().strftime("%Y%m%d_%H%M")
    logfile = f"outputs/logs/extract-storms-{execution_time}.log"

    logger = set_up_logger(filename=logfile)
    logger.setLevel(logging.INFO)

    args = sys.argv

    start = args[1]
    duration = args[2]
    watershed_name = args[3]
    domain_version = args[4]
    domain_uri = args[5]
    watershed_uri = args[6]
    dss_dir = args[7]
    png_dir = args[8]
    scale_max = args[9]
    index_name = args[10]

    logging.info(
        json.dumps(
            {
                "event_date": start,
                "job": "main",
                "status": "start",
                "params": {
                    "start": start,
                    "duration": duration,
                    "watershed_name": watershed_name,
                    "domain_version": domain_version,
                    "domain_uri": domain_uri,
                    "watershed_uri": watershed_uri,
                    "dss_dir": dss_dir,
                    "png_dir": png_dir,
                    "scale_max": scale_max,
                    "index_name": index_name,
                },
            }
        )
    )
    try:
        main(
            start,
            duration,
            watershed_name,
            domain_version,
            domain_uri,
            watershed_uri,
            dss_dir,
            png_dir,
            scale_max,
            index_name,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start,
                    "job": "main",
                    "status": "success",
                    "params": {
                        "start": start,
                        "duration": duration,
                        "watershed_name": watershed_name,
                        "domain_version": domain_version,
                        "domain_uri": domain_uri,
                        "watershed_uri": watershed_uri,
                        "dss_dir": dss_dir,
                        "png_dir": png_dir,
                        "scale_max": scale_max,
                        "index_name": index_name,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start,
                    "job": "main",
                    "status": "failed",
                    "params": {
                        "start": start,
                        "duration": duration,
                        "watershed_name": watershed_name,
                        "domain_version": domain_version,
                        "domain_uri": domain_uri,
                        "watershed_uri": watershed_uri,
                        "dss_dir": dss_dir,
                        "png_dir": png_dir,
                        "scale_max": scale_max,
                        "index_name": index_name,
                    },
                    "error": str(e),
                }
            )
        )

    log_to_json(logfile)
