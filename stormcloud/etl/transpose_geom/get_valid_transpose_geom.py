import datetime
import json
import pickle

import shapely.geometry
import shapely.wkt
from storms.cluster import get_xr_dataset, s3_geometry_reader
from storms.transpose import Transposer

START_DATE = datetime.datetime(2012, 1, 15)
DURATION = 1
DATA_TYPE = "precipitation"
AGG_METHOD = "sum"


def create_dummy_transposer(watershed_uri: str, transposition_uri: str, session, **kwargs) -> Transposer:
    watershed_geom = s3_geometry_reader(session, watershed_uri)
    transposition_geom = s3_geometry_reader(session, transposition_uri)
    if kwargs.get("pickled_dataset_filepath"):
        pickled_dataset_filepath = kwargs["pickled_dataset_filepath"]
        xsum = pickle.load(pickled_dataset_filepath)
    else:
        xsum = get_xr_dataset(DATA_TYPE, START_DATE, DURATION, AGG_METHOD, transposition_geom)
        if kwargs.get("output_pickle_filepath"):
            output_path = kwargs["output_pickle_filepath"]
            with open(output_path, "w") as f:
                pickle.dump(xsum, f)

    return Transposer(xsum, watershed_geom)


def save_geom(transposer: Transposer, out_geojson_path: str) -> None:
    valid_geom = transposer.valid_space_geom()
    geojson_object = shapely.geometry.mapping(valid_geom)

    with open(out_geojson_path, "w") as f:
        json.dump(geojson_object, f)


def main(watershed_uri: str, transposition_uri: str, out_geojson_path: str, session, **kwargs):
    transposer = create_dummy_transposer(watershed_uri, transposition_uri, session, **kwargs)
    save_geom(transposer, out_geojson_path)


if __name__ == "__main__":
    import argparse
    import os

    import boto3
    from dotenv import find_dotenv, load_dotenv

    parser = argparse.ArgumentParser()

    parser.add_argument("watershed_uri", type=str, help="s3 URI of geojson associated with watershed of interest")
    parser.add_argument(
        "transposition_uri", type=str, help="s3 URI of geojson associated with transposition domain of interest"
    )
    parser.add_argument("out_geojson_path", type=str, help="output path for geojson of valid transposition areas")
    parser.add_argument(
        "--p",
        "--pickled_dataset_filepath",
        type=str,
        required=False,
        default=None,
        help="filepath of pickled xarray dataset to use when creating dummy transposer for area creation",
    )
    parser.add_argument(
        "--o",
        "--output_pickle_filepath",
        type=str,
        required=False,
        default=None,
        help="filepath to which pickled dataset should be saved when loading dataset in transposer creation",
    )

    load_dotenv(find_dotenv())
    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])

    args = parser.parse_args()
    args_dict = vars(args)
    args_dict["session"] = session

    main(**args_dict)
