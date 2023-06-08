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


def query_ms(
    ms_client: Client,
    watershed_name: str,
    domain_name: str,
    mean_filter: float,
    limit: int,
    declustered: bool = True,
    top_by_year: Union[int, None] = None,
) -> list:
    """Queries meilisearch database for storm metadata

    Args:
        ms_client (Client): Meilisearch client
        watershed_name (str): Watershed name. Should be one that exists in meilisearch database attribute metadata.watershed_name
        domain_name (str): Domain name. Should be one that exists in meilisearch database attribute metadata.transposition_domain_name
        mean_filter (float): Minimum mean precipitation amount to use as filter when retrieving storms
        limit (int, optional): Limit for total DSS files fetched
        declustered (bool, optional): If true, use declustered rank to determine top storms by precipitation. If false, use true rank. Defaults to True.
        top_by_year (Union[int, None], optional): If integer is provided, will get that many storms per year, selecting them by highest precipitation. If none is provided, will fetch overall highest storms without stratifying by year. Defaults to None.

    Returns:
        list: Documents retrieved from meilisearch database
    """
    filter_list = [
        f"stats.mean >= {mean_filter}",
        f'metadata.watershed_name = "{watershed_name}"',
        f'metadata.transposition_domain_name = "{domain_name}"',
    ]
    if declustered:
        filter_list.append("ranks.declustered_rank > 0")
    if top_by_year != None:
        if declustered:
            filter_list.append(f"ranks.declustered_rank <= {top_by_year}")
        else:
            filter_list.append(f"rank.true_rank <= {top_by_year}")
    query_params = {
        "filter": filter_list,
        "limit": limit,
        "sort": ["stats.mean:desc", "start.timestamp:asc"],
    }
    results = ms_client.index(INDEX).search("", query_params)
    hits = results.get("hits")
    return hits


def main(
    watershed_name: str,
    domain_name: str,
    transpose_name: str,
    data_directory: Union[str, None] = None,
    mean_filter: float = 0,
    top_by_year: int = 10,
    limit: int = 1000,
):
    """Function which uses documents in meilisearch database to fetch DSS files for storms which have top mean precipitation from s3 and package into a .grid file format

    Args:
        watershed_name (str): Watershed name. Should be one that exists in meilisearch database attribute metadata.watershed_name
        domain_name (str): Domain name. Should be one that exists in meilisearch database attribute metadata.transposition_domain_name
        transpose_name (str): Name used in grid file 'Grid Manager' parameter and used to name grid file
        data_directory (Union[str, None], optional): Directory to which DSS file and grid files are saved. If none provided, will be created using watershed name. Defaults to None.
        mean_filter (float, optional): Minimum mean precipitation amount to use as filter when retrieving storms. Defaults to 0.
        top_by_year (int, optional): How many storms per year to be retrieved, selected by highest precipitation. Defaults to 10.
        limit (int, optional): Limit for total DSS files fetched. Defaults to 1000.
    """
    # Create s3 and meilisearch clients from environment variables
    session = boto3.session.Session()
    bucket_name = os.environ["S3_BUCKET_NAME"]
    s3_client = session.client("s3")
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])

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
    docs = query_ms(ms_client, watershed_name, domain_name, mean_filter, limit, top_by_year=top_by_year)

    # Make grid file
    with open(grid_file, "w") as gridf:
        gridf.write(f"Grid Manager: {transpose_name}\n")
        gridf.write("     Version: 4.11\n")
        gridf.write("     Filepath Separator: \\\n")
        gridf.write("End:\n\n")

        for i, doc in enumerate(docs):
            s3_uri = os.path.join(
                doc["metadata"]["transposition_domain_source"].replace(".geojson", ""),
                f"{doc['duration']}h",
                "dss",
                f"{doc['id'].split('_')[-1]}.dss",
            )
            s3_key = s3_uri.replace(f"s3://{bucket_name}/", "")
            dss_name = f"{doc['id'].split('_')[-1]}_Y{doc['ranks']['declustered_rank']:03}_T{i+1:03}.dss"
            dss_path = os.path.join(dss_dir, dss_name)

            s3_client.download_file(bucket_name, s3_key, dss_path)

            with HecDss.Open(dss_path) as dss:
                dss_pathname = dss.getPathnameList("/*/*/*/*/*/*/", sort=1)[0]

            dss_filename = os.path.join("C:/Data", dss_path).replace("/", "\\")

            last_modified = s3_client.head_object(Bucket=bucket_name, Key=s3_key)["LastModified"]
            last_modified_date = datetime.strftime(last_modified, "%d %B %Y")
            last_modified_time = datetime.strftime(last_modified, "%H:%M:%S")

            # transform center to SHG
            wgs_point = Point(doc["geom"]["center_x"], doc["geom"]["center_y"])
            shg_point = transform(project, wgs_point)

            # get grid info
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

            # write grid info
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
    parser.add_argument("transpose_name", type=str, help="Transpose name to use in grid file creation")
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
        "-t", "--top_by_year", default=10, type=int, required=False, help="How many storms to select per year of record"
    )

    args = parser.parse_args()

    main(args.watershed_name, args.domain_name, args.transpose_name, args.data_directory, args.mean_filter, args.limit)
