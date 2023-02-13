import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
import json
from logger import set_up_logger, log_to_json
import logging
import os
from storms.utils import plotter, ms, batch
import sys
from storms.cluster import (
    get_xr_dataset,
    write_dss,
    s3_geometry_reader,
    get_atlas14,
)
from storms.transpose import Transposer

# for local testing
# load_dotenv(find_dotenv())
# session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
# s3_client = session.client("s3")

# for batch production
logging.getLogger("botocore").setLevel(logging.WARNING)
os.environ.update(batch.get_secrets(secret_name="stormcloud-secrets", region_name="us-east-1"))
session = boto3.session.Session()
s3_client = session.client("s3")

# get rid of dss logger
logging.getLogger("pydsstools").setLevel(logging.ERROR)


def main(
    start: str,
    duration: int,
    watershed_name: str,
    domain_name: str,
    domain_uri: str,
    watershed_uri: str,
    atlas14_uri: str = None,
    dss_dir: str = "./",
    png_dir: str = "./",
    doc_dir: str = "./",
    scale_max: float = 300,
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

    # get atlas 14 data for normalizing (ignore if uri is None)
    if atlas14_uri is None:
        norm_arr = None
    else:
        try:
            xnorm = get_atlas14(atlas14_uri, xsum.APCP_surface)
            norm_arr = xnorm.to_numpy() * 25.4  # convert to mm

            logging.info(
                json.dumps(
                    {
                        "event_date": start.strftime("%Y-%m-%d"),
                        "job": get_atlas14.__name__,
                        "status": "success",
                        "params": {
                            "s3_uri": atlas14_uri,
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
                            "s3_uri": atlas14_uri,
                            "interpolate_to": "xsum",
                        },
                        "error": str(e),
                    }
                )
            )
            raise

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
                        "normalized_data": atlas14_uri,
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
                        "normalized_data": atlas14_uri,
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
            start, duration, watershed_name, domain_name, watershed_uri, domain_uri, best_translate
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
                        "domain_name": domain_name,
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
                        "domain_name": domain_name,
                        "watershed_uri": watershed_uri,
                        "domain_uri": domain_uri,
                        "transpose": best_translate.to_dict(),
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # write document to json
    doc_path = os.path.join(doc_dir, f"{start_as_str}.json")
    try:
        doc.write_to(doc_path)
        logging.info(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": doc.write_to.__name__,
                    "status": "success",
                    "params": {"file_path": doc_path},
                }
            )
        )

    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start.strftime("%Y-%m-%d"),
                    "job": doc.write_to.__name__,
                    "status": "failed",
                    "params": {"file_path": doc_path},
                    "error": str(e),
                }
            )
        )
        raise

    return png_path, dss_path, doc_path


if __name__ == "__main__":

    logger = set_up_logger()
    logger.setLevel(logging.INFO)

    args = sys.argv

    # required args
    start = args[1]
    kwargs = {
        "start": args[1],
        "duration": int(args[2]),
        "watershed_name": args[3],
        "domain_name": args[4],
        "domain_uri": args[5],
        "watershed_uri": args[6],
    }
    s3_bucket = args[7]
    s3_key_prefix = args[8]

    # optional args
    # atlas14_uri = args[9] if len(args) > 9 else None
    if len(args) > 9:
        kwargs["atlas14_uri"] = args[9]
    if len(args) > 10:
        kwargs["dss_dir"] = args[10]
    if len(args) > 11:
        kwargs["png_dir"] = args[11]
    if len(args) > 12:
        kwargs["doc_dir"] = args[12]
    if len(args) > 13:
        kwargs["scale_max"] = args[13]

    logging.info(
        json.dumps(
            {
                "event_date": start,
                "job": "main",
                "status": "start",
                "params": kwargs,
            }
        )
    )
    try:
        # get 3 files: dss, png, json
        png_path, dss_path, doc_path = main(**kwargs)
        logging.info(
            json.dumps(
                {
                    "event_date": start,
                    "job": "main",
                    "status": "success",
                    "params": kwargs,
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
                    "params": kwargs,
                    "error": str(e),
                }
            )
        )
        raise

    # write png to s3
    try:
        png_key = os.path.join(s3_key_prefix, "pngs", os.path.basename(png_path))
        s3_client.upload_file(png_path, s3_bucket, png_key)

        logging.info(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": png_path,
                        "bucket": s3_bucket,
                        "object_name": png_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": png_path,
                        "bucket": s3_bucket,
                        "object_name": png_key,
                    },
                    "error": str(e),
                }
            )
        )

    # write dss to s3
    try:
        dss_key = os.path.join(s3_key_prefix, "dss", os.path.basename(dss_path))
        s3_client.upload_file(dss_path, s3_bucket, dss_key)

        logging.info(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": dss_path,
                        "bucket": s3_bucket,
                        "object_name": dss_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": dss_path,
                        "bucket": s3_bucket,
                        "object_name": dss_key,
                    },
                    "error": str(e),
                }
            )
        )

    # write doc to s3
    try:
        doc_key = os.path.join(s3_key_prefix, "docs", os.path.basename(doc_path))
        s3_client.upload_file(doc_path, s3_bucket, doc_key)

        logging.info(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": doc_path,
                        "bucket": s3_bucket,
                        "object_name": doc_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": doc_path,
                        "bucket": s3_bucket,
                        "object_name": doc_key,
                    },
                    "error": str(e),
                }
            )
        )
