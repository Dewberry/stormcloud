"""
Main script of repo. Uses s3 resources for watershed and transposition region to define
valid transpositions of a watershed, then calculates accumulation statistics for all valid
tranpositions. Selects the maximum accumulation period found and saves to DSS file along
with PNG and summary statistics
"""

import enum
import json
import logging
import os
from datetime import datetime
from typing import Any, Tuple

import boto3
from dotenv import find_dotenv, load_dotenv

from storms.cluster import get_atlas14, get_xr_dataset, s3_geometry_reader, write_dss
from storms.transpose import Transposer
from storms.utils import batch, ms, plotter

# Set constants
STORM_DATA_TYPE = "precipitation"
MM_TO_INCH_CONVERSION_FACTOR = 0.03937007874015748


class RunSetting(enum.Enum):
    LOCAL = enum.auto()
    BATCH = enum.auto()


def get_client_session(setting: RunSetting = RunSetting.BATCH) -> "Tuple[Any, Any]":
    """Gets session and s3 client in a tuple, using different methods depending on if in batch or local development environment

    Args:
        setting (RunSetting, optional): Setting determining how to get session and client. Defaults to RunSetting.BATCH.

    Returns:
        Tuple[Any, Any]: Tuple of session and client, in that order
    """
    if setting == RunSetting.LOCAL:
        # for local testing
        load_dotenv(find_dotenv())
        session = boto3.session.Session(
            os.environ["AWS_ACCESS_KEY_ID"],
            os.environ["AWS_SECRET_ACCESS_KEY"],
            region_name=os.environ["AWS_REGION"],
        )
        s3_client = session.client("s3")
    elif setting == RunSetting.BATCH:
        # for batch production
        logging.getLogger("botocore").setLevel(logging.WARNING)
        os.environ.update(
            batch.get_secrets(secret_name="stormcloud-secrets", region_name="us-east-1")
        )
        session = boto3.session.Session()
        s3_client = session.client("s3")
    return session, s3_client


def main(
    start_date: str,
    hours_duration: int,
    watershed_name: str,
    watershed_uri: str,
    domain_name: str,
    domain_uri: str,
    session: Any,
    atlas14_uri: str = None,
    scale_max: float = 12,
    dss_dir: str = "./",
    png_dir: str = "./",
    doc_dir: str = "./",
) -> "Tuple[str, str, str]":
    """Identifies maximum precipitation accumulation for watershed transposition within transposition region

    Args:
        start_date (str): Start of duration window. Should be date in format YYYY-mm-dd
        hours_duration (int): Hour length of analysis window
        watershed_name (str): Watershed name
        watershed_uri (str): s3 path of watershed geojson
        domain_name (str): Transposition region version name
        domain_uri (str): s3 path of transposition region geojson
        session (Any): s3 session for interactions with s3
        atlas14_uri (str, optional): s3 path to ATLAS14 raster to use in normalization. Defaults to None.
        scale_max (float, optional): Maxmimum precipitation in inches to use in plotting. Defaults to 12.
        dss_dir (str, optional): Relative directory to use when saving dss files. Defaults to "./".
        png_dir (str, optional): Relative directory to use when saving png files. Defaults to "./".
        doc_dir (str, optional): Relative directory to use when saving json files documenting model statistics. Defaults to "./".

    Returns:
        Tuple[str, str, str]: Tuple of paths to resources in following order: [png, dss, json doc]
    """
    # convert start to datetime
    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
    # convert to string of specified format for use in file naming
    start_str = start_dt.strftime("%Y%m%d")

    # read in watershed geometry and transposition domain geometry (shapely polygons)
    try:
        transposition_geom = s3_geometry_reader(session, domain_uri)
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
        xdata = get_xr_dataset(
            STORM_DATA_TYPE, start_dt, hours_duration, mask=transposition_geom
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {
                        "data_type": STORM_DATA_TYPE,
                        "start": start_dt.strftime("%Y-%m-%d"),
                        "duration": hours_duration,
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": get_xr_dataset.__name__,
                    "status": "failed",
                    "params": {
                        "data_type": STORM_DATA_TYPE,
                        "start": start_dt.strftime("%Y-%m-%d"),
                        "duration": hours_duration,
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
        xsum = get_xr_dataset(
            STORM_DATA_TYPE,
            start_dt,
            hours_duration,
            aggregate_method=aggregate_method,
            mask=transposition_geom,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": get_xr_dataset.__name__,
                    "status": "success",
                    "params": {
                        "data_type": STORM_DATA_TYPE,
                        "start": start_dt.strftime("%Y-%m-%d"),
                        "duration": hours_duration,
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": get_xr_dataset.__name__,
                    "status": "failed",
                    "params": {
                        "data_type": STORM_DATA_TYPE,
                        "start": start_dt.strftime("%Y-%m-%d"),
                        "duration": hours_duration,
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
            norm_arr = xnorm.to_numpy()

            logging.info(
                json.dumps(
                    {
                        "event_date": start_dt.strftime("%Y-%m-%d"),
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
                        "event_date": start_dt.strftime("%Y-%m-%d"),
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
        transposer = Transposer(
            xsum,
            watershed_geom,
            normalized_data=norm_arr,
            multiplier=MM_TO_INCH_CONVERSION_FACTOR,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": Transposer.__name__,
                    "status": "success",
                    "params": {
                        "xsum": "xsum",
                        "watershed_geom": watershed_uri,
                        "data_var": "APCP_surface",
                        "x_var": "longitude",
                        "y_var": "latitude",
                        "normalized_data": atlas14_uri,
                        "multiplier": MM_TO_INCH_CONVERSION_FACTOR,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": Transposer.__name__,
                    "status": "failed",
                    "params": {
                        "xsum": "xsum",
                        "watershed_geom": watershed_uri,
                        "data_var": "APCP_surface",
                        "x_var": "longitude",
                        "y_var": "latitude",
                        "normalized_data": atlas14_uri,
                        "multiplier": MM_TO_INCH_CONVERSION_FACTOR,
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                        "event_date": start_dt.strftime("%Y-%m-%d"),
                        "job": "get_best_translate",
                        "message": f"{len(translates)} tied for mean",
                    }
                )
            )
            translates = translates[
                max_ranks[mean_ranks == 1] == max_ranks[mean_ranks == 1].min()
            ]

            if len(translates) > 1:
                logging.warning(
                    json.dumps(
                        {
                            "event_date": start_dt.strftime("%Y-%m-%d"),
                            "job": "get_best_translate",
                            "message": f"{len(translates)} tied for mean and max",
                        }
                    )
                )

        best_translate = translates[0]
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": "get_best_translate",
                    "status": "success",
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
    png_path = os.path.join(png_dir, f"{start_str}.png")
    scale_label = "Accumulation (Inches)"
    scale_min = 0
    try:
        plotter.cluster_plot(
            xsum,
            translate_geom,
            scale_min,
            scale_max,
            scale_label,
            multiplier=MM_TO_INCH_CONVERSION_FACTOR,
            geom=[watershed_geom, transposer.valid_space_geom()],
            png=png_path,
        )

        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": plotter.cluster_plot.__name__,
                    "status": "success",
                    "params": {
                        "xdata": "xsum",
                        "cluster_geometry": "translate_geom",
                        "vmin": scale_min,
                        "vmax": scale_max,
                        "scale_label": scale_label,
                        "multiplier": MM_TO_INCH_CONVERSION_FACTOR,
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": plotter.cluster_plot.__name__,
                    "status": "failed",
                    "params": {
                        "xdata": "xsum",
                        "cluster_geometry": "translate_geom",
                        "vmin": scale_min,
                        "vmax": scale_max,
                        "scale_label": scale_label,
                        "multiplier": MM_TO_INCH_CONVERSION_FACTOR,
                        "geom": ["watershed_geom", "transposer.valid_space_geom"],
                        "png": png_path,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # write grid to dss
    dss_path = os.path.join(dss_dir, f"{start_str}.dss")
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
            start_dt,
            hours_duration,
            watershed_name,
            domain_name,
            watershed_uri,
            domain_uri,
            best_translate,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": ms.tranpose_to_doc.__name__,
                    "status": "success",
                    "params": {
                        "event_start": str(start_dt),
                        "duration": hours_duration,
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
                    "job": ms.tranpose_to_doc.__name__,
                    "status": "failed",
                    "params": {
                        "event_start": str(start_dt),
                        "duration": hours_duration,
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
    doc_path = os.path.join(doc_dir, f"{start_str}.json")
    try:
        doc.write_to(doc_path)
        logging.info(
            json.dumps(
                {
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
                    "event_date": start_dt.strftime("%Y-%m-%d"),
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
    import argparse
    import logging

    from logger import set_up_logger

    logger = set_up_logger()
    logger.setLevel(logging.INFO)

    parser = argparse.ArgumentParser(
        prog="Storm Extractor (V2)",
        description="Calculates valid transposition of a watershed within a transposition region which has greatest accumulated precipitation over a specified period",
        usage="Example usage: python extract_storms_v2.py -s '1985-05-10' -hr 72 -w Duwamish -wu s3://tempest/watersheds/duwamish/duwamish.geojson -d V01 -du s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01.geojson -b tempest -p watersheds/duwamish/duwamish-transpo-area-v01/72h",
    )

    parser.add_argument(
        "-s",
        "--start_date",
        type=str,
        required=True,
        help="Start date for model. Should be in format YYYY-mm-dd, like 2023-05-10 for May 10, 2023",
    )

    parser.add_argument(
        "-hr",
        "--hours_duration",
        type=int,
        required=True,
        help="Number of hours to use as accumulation window from start date",
    )

    parser.add_argument(
        "-w",
        "--watershed_name",
        type=str,
        required=True,
        help="Name of watershed for which model will be run",
    )

    parser.add_argument(
        "-wu",
        "--watershed_uri",
        type=str,
        required=True,
        help="s3 path to geojson file for watershed extent",
    )

    parser.add_argument(
        "-d",
        "--domain_name",
        type=str,
        required=True,
        help="Name for version of transposition region used",
    )

    parser.add_argument(
        "-du",
        "--domain_uri",
        type=str,
        required=True,
        help="s3 path to geojson file for transposition region used in model",
    )

    parser.add_argument(
        "-b",
        "--s3_bucket",
        type=str,
        required=True,
        help="s3 bucket to use for storing output files",
    )

    parser.add_argument(
        "-p",
        "--s3_prefix",
        type=str,
        required=True,
        help="Prefix to prepend to keys when saving output files to s3 bucket",
    )

    parser.add_argument(
        "-a",
        "--atlas14_uri",
        default=None,
        type=str,
        required=False,
        help="s3 path to atlas14 raster data used in normalization of precipitation values. Defaults to None",
    )

    parser.add_argument(
        "-sm",
        "--scale_max",
        default=12,
        type=float,
        required=False,
        help="Maximum precipitation value in inches to use in plotting. Defaults to 12 inches.",
    )

    parser.add_argument(
        "-dss",
        "--dss_directory",
        default="./",
        type=str,
        required=False,
        help="Relative path to directory to use when saving DSS files generated by model. Defaults to current directory.",
    )

    parser.add_argument(
        "-png",
        "--png_directory",
        default="./",
        type=str,
        required=False,
        help="Relative path to directory to use when saving PNG files generated by model. Defaults to current directory",
    )

    parser.add_argument(
        "-doc",
        "--doc_directory",
        default="./",
        type=str,
        required=False,
        help="Relative path to directory to use when saving JSON statistic files generated by model. Defaults to current directory",
    )

    parser.add_argument(
        "-rs",
        "--run_setting",
        default="BATCH",
        type=str,
        required=False,
        choices=["BATCH", "LOCAL"],
        help="Environment of script. Either LOCAL or BATCH. Defaults to BATCH",
    )

    args = parser.parse_args()

    # convert run setting string to RunSetting enum class
    if args.run_setting == "BATCH":
        run_setting = RunSetting.BATCH
    elif args.run_setting == "LOCAL":
        run_setting = RunSetting.LOCAL
    else:
        raise ValueError(
            f"Unexpected run setting. Expected LOCAL or BATCH, got {args.run_setting}"
        )

    # get session and client
    session, s3_client = get_client_session(run_setting)

    # convert args to dict for logging
    args_dict = {key: value for key, value in args._get_kwargs()}

    logging.info(
        json.dumps(
            {
                "event_date": args.start_date,
                "job": "main",
                "status": "start",
                "params": args_dict,
            }
        )
    )
    try:
        # get 3 files: dss, png, json
        png_path, dss_path, doc_path = main(
            args.start_date,
            args.hours_duration,
            args.watershed_name,
            args.watershed_uri,
            args.domain_name,
            args.domain_uri,
            session,
            args.atlas14_uri,
            args.scale_max,
            args.dss_directory,
            args.png_directory,
            args.doc_directory,
        )
        logging.info(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": "main",
                    "status": "success",
                    "params": args_dict,
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": "main",
                    "status": "failed",
                    "params": args_dict,
                    "error": str(e),
                }
            )
        )
        raise

    # write png to s3
    try:
        png_key = os.path.join(args.s3_prefix, "pngs", os.path.basename(png_path))
        s3_client.upload_file(png_path, args.s3_bucket, png_key)

        logging.info(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": png_path,
                        "bucket": args.s3_bucket,
                        "object_name": png_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": png_path,
                        "bucket": args.s3_bucket,
                        "object_name": png_key,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # write dss to s3
    try:
        dss_key = os.path.join(args.s3_prefix, "dss", os.path.basename(dss_path))
        s3_client.upload_file(dss_path, args.s3_bucket, dss_key)

        logging.info(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": dss_path,
                        "bucket": args.s3_bucket,
                        "object_name": dss_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": dss_path,
                        "bucket": args.s3_bucket,
                        "object_name": dss_key,
                    },
                    "error": str(e),
                }
            )
        )
        raise

    # write doc to s3
    try:
        doc_key = os.path.join(args.s3_prefix, "docs", os.path.basename(doc_path))
        s3_client.upload_file(doc_path, args.s3_bucket, doc_key)

        logging.info(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "success",
                    "params": {
                        "file_name": doc_path,
                        "bucket": args.s3_bucket,
                        "object_name": doc_key,
                    },
                }
            )
        )
    except Exception as e:
        logging.error(
            json.dumps(
                {
                    "event_date": args.start_date,
                    "job": s3_client.upload_file.__name__,
                    "status": "failed",
                    "params": {
                        "file_name": doc_path,
                        "bucket": args.s3_bucket,
                        "object_name": doc_key,
                    },
                    "error": str(e),
                }
            )
        )
        raise
