""" Uses documents on meilisearch and DSS file on s3 to create a grid file in desired format """
import os
from datetime import datetime
from typing import Union

import boto3
import pyproj
from constants import INDEX
from meilisearch import Client
from pydsstools.heclib.dss import HecDss
from pydsstools.heclib.utils import SHG_WKT
from shapely.geometry import Point
from shapely.ops import transform
from storm_query import query_ms


def strip_all_whitespace(dirty_string: str) -> str:
    """Strips all whitespace from string

    Args:
        dirty_string (str): String with whitespace

    Returns:
        str: String stripped of whitespace
    """
    not_space = lambda x: not str.isspace(x)
    cleaned = "".join(filter(not_space, dirty_string))
    return cleaned


def create_grid_name(data_directory: str, transpose_name: str) -> str:
    """Creates output filepath for grid file based on transpose name and data output directory

    Args:
        data_directory (str): Data directory to which grid file should be saved
        transpose_name (str): Transpose name

    Returns:
        str: Cleaned transpose file name
    """
    simple_stripped = transpose_name.strip()
    cleaned = simple_stripped.replace(".", "")
    underscored = cleaned.replace(" ", "_")
    full_stripped = strip_all_whitespace(underscored)
    grid_basenme = f"{full_stripped}.grid"
    return os.path.join(data_directory, grid_basenme)


def main(
    bucket_name: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
    watershed_name: str,
    domain_name: str,
    transpose_name: str,
    data_directory: Union[str, None] = None,
    mean_filter: float = 0,
    top_by_year: int = 10,
    filter_year: Union[int, None] = None,
    limit: int = 1000,
):
    # Create s3 and meilisearch clients from environment variables
    session = boto3.session.Session(access_key_id, secret_access_key)
    s3_client = session.client("s3")
    ms_client = Client(ms_host, ms_api_key)

    # Create data directory from watershed if none provided
    if data_directory == None:
        data_directory = strip_all_whitespace(watershed_name)
    # Create output grid file name
    grid_file = create_grid_name(data_directory, transpose_name)
    # Create output directory for DSS files
    dss_dir = os.path.join(data_directory, "dss")
    # Ensure output directories exist
    for directory_path in [data_directory, dss_dir]:
        os.makedirs(directory_path, exist_ok=True)

    # Create transformation to apply to center point fetched from storm model metadata
    wgs84 = pyproj.CRS("EPSG:4326")
    project = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform

    # Search meilisearch database for top storms by mean precipitation
    docs = query_ms(
        ms_client,
        INDEX,
        watershed_name,
        domain_name,
        mean_filter,
        limit,
        top_by_year=top_by_year,
        filter_year=filter_year,
    )
    if len(docs) == 0:
        raise ValueError("No records found in meilisearch database")

    # Make grid file
    with open(grid_file, "w") as gridf:
        gridf.write(f"Grid Manager: {transpose_name}\n")
        gridf.write("     Version: 4.11\n")
        gridf.write("     Filepath Separator: \\\n")
        gridf.write("End:\n\n")

        for i, doc in enumerate(docs):
            # Get s3 key for dss file
            s3_uri = os.path.join(
                doc["metadata"]["transposition_domain_source"].replace(".geojson", ""),
                f"{doc['duration']}h",
                "dss",
                f"{doc['id'].split('_')[-1]}.dss",
            )
            s3_key = s3_uri.replace(f"s3://{bucket_name}/", "")

            # Get target filename to use in dss download
            dss_name = f"{doc['id'].split('_')[-1]}_Y{doc['ranks']['declustered_rank']:03}_T{i+1:03}.dss"
            dss_path = os.path.join(dss_dir, dss_name)

            # Download file
            s3_client.download_file(bucket_name, s3_key, dss_path)

            # Get first pathname from dss file
            with HecDss.Open(dss_path) as dss:
                dss_pathname = dss.getPathnameList("/*/*/*/*/*/*/", sort=1)[0]

            # Modify the filename to fit expectations
            dss_filename = os.path.join("C:/Data", dss_path).replace("/", "\\")

            # Get last modification date and time from s3 key
            last_modified = s3_client.head_object(Bucket=bucket_name, Key=s3_key)[
                "LastModified"
            ]
            last_modified_date = datetime.strftime(last_modified, "%d %B %Y")
            last_modified_time = datetime.strftime(last_modified, "%H:%M:%S")

            # Transform center to SHG
            wgs_point = Point(doc["geom"]["center_x"], doc["geom"]["center_y"])
            shg_point = transform(project, wgs_point)

            # Get / set grid info
            grid = f"{doc['metadata']['source']} {doc['start']['datetime'].split(' ')[0]} Y{doc['ranks']['declustered_rank']:03} T{i+1:03}"
            grid_type = "Precipitation"
            ref_height_units = "Meters"
            ref_height = "10.0"
            data_source_type = "External DSS"
            variant = "Variant-1"
            default_variant = "Yes"
            end_variant = "Variant-1"
            use_lookup_table = "No"
            storm_center_x = shg_point.x
            storm_center_y = shg_point.y

            # Write grid info
            gridf.write(f"Grid: {grid}\n")
            gridf.write(f"     Grid Type: {grid_type}\n")
            gridf.write(f"     Last Modified Date: {last_modified_date}\n")
            gridf.write(f"     Last Modified Time: {last_modified_time}\n")
            gridf.write(f"     Reference Height Units: {ref_height_units}\n")
            gridf.write(f"     Reference Height: {ref_height}\n")
            gridf.write(f"     Data Source Type: {data_source_type}\n")
            gridf.write(f"     Variant: {variant}\n")
            gridf.write(f"       Last Variant Modified Date: {last_modified_date}\n")
            gridf.write(f"       Last Variant Modified Time: {last_modified_time}\n")
            gridf.write(f"       Default Variant: {default_variant}\n")
            gridf.write(f"       DSS File Name: {dss_filename}\n")
            gridf.write(f"       DSS Pathname: {dss_pathname}\n")
            gridf.write(f"     End Variant: {end_variant}\n")
            gridf.write(f"     Use Lookup Table: {use_lookup_table}\n")
            gridf.write(f"     Storm Center X: {storm_center_x}\n")
            gridf.write(f"     Storm Center Y: {storm_center_y}\n")
            gridf.write(f"End:\n\n")


if __name__ == "__main__":
    import argparse

    from dotenv import load_dotenv

    load_dotenv()

    bucket_name = os.environ["S3_BUCKET_NAME"]
    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    parser = argparse.ArgumentParser(
        prog="HMS Grid Creator",
        usage="python ms/create-hms-grid.py 'Indian Creek' v01 'IC Transpose'",
        description="Creates HMS grid from DSS files which are in the top storms within a meilisearch database of storms for a specific watershed and domain",
    )
    parser.add_argument(
        "watershed_name",
        type=str,
        help="Watershed name, should correspond to a valid value of the watershed_name field of the meilisearch database",
    )
    parser.add_argument(
        "domain_name",
        type=str,
        help="Domain name, should correspond to a valid value of the transposition_domain_name field of the meilisearch database",
    )
    parser.add_argument(
        "transpose_name",
        type=str,
        help="Transpose name to use in grid file creation",
    )
    parser.add_argument(
        "-d",
        "--data_directory",
        default=None,
        type=str,
        required=False,
        help="Local directory to use for downloading DSS file and writing grid file",
    )
    parser.add_argument(
        "-m",
        "--mean_filter",
        default=0,
        type=float,
        required=False,
        help="Mean precipitation value to use in filtering storms for selection",
    )
    parser.add_argument(
        "-l",
        "--limit",
        default=1000,
        type=int,
        required=False,
        help="Maximum number of storms to retrieve from the meilisearch database",
    )
    parser.add_argument(
        "-t",
        "--top_by_year",
        default=10,
        type=int,
        required=False,
        help="How many storms to select per year of record",
    )
    parser.add_argument(
        "-y",
        "--filter_year",
        default=None,
        type=int,
        required=False,
        help="Year to filter by when searching",
    )

    args = parser.parse_args()

    main(
        bucket_name,
        access_key_id,
        secret_access_key,
        ms_host,
        ms_api_key,
        args.watershed_name,
        args.domain_name,
        args.transpose_name,
        args.data_directory,
        args.mean_filter,
        args.top_by_year,
        args.filter_year,
        args.limit,
    )
