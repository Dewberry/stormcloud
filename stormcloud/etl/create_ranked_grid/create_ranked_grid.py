"""
ETL to query meilisearch for top n storms per year for a watershed and transposition domain, then format the metadata into the expected format for hms grid creation, then created a zipped grid package and upload to s3
"""

import logging
import os
from shutil import make_archive
from tempfile import TemporaryDirectory
from typing import Any, Callable, Iterable, Iterator

import pyproj
from meilisearch import Client
from pydsstools.heclib.utils import SHG_WKT

from ms.storm_query import query_ms
from ms.constants import INDEX
from create_hms_grid import prepare_structure, insert_meta_into_grid, GridWriter
from construct_meta import construct_dss_meta, guess_dss_uri
from common.cloud import split_s3_path
from common.dss import DSSProductMeta


def create_grid_filename(watershed: str) -> str:
    initials = ""
    for char in watershed:
        if char.isupper():
            initials += char
    return f"{initials}_Transpose.grid"

def create_transform(tgt_epsg: str = "EPSG:4326") -> Callable:
    wgs84 = pyproj.CRS("EPSG:4326")
    transform_function = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform
    return transform_function

def get_ranked_documents(ms_client: Client, s3_client: Any, watershed_name: str, domain_name: str, top_by_year: int, mean_filter: float = 0, limit: int = 1000, declustered: bool = True) -> Iterator[DSSProductMeta]:
    docs = query_ms(ms_client, INDEX, watershed_name, domain_name, mean_filter, limit, declustered, top_by_year)
    for doc in docs:
        dss_uri = guess_dss_uri(doc["metadata"]["transposition_domain_source"], doc["start"]["datetime"], doc["duration"])
        transform_function = create_transform()
        meta = construct_dss_meta(doc["metadata"]["watershed_name"], doc["metadata"]["transposition_domain_source"], dss_uri, doc["start"]["datetime"], None, None, doc["duration"], doc["geom"]["center_x"], doc["geom"]["center_y"], doc["ranks"]["true_rank"], doc["ranks"]["declustered_rank"], limit, top_by_year, s3_client, transform_function=transform_function)
        yield meta

def write_meta_to_grid(grid_directory: str, grid_file_basename: str, dss_meta_iterable: Iterable[DSSProductMeta], s3_client: Any) -> str:
    prepare_structure(grid_directory)
    grid_filename = os.path.join(grid_directory, grid_file_basename)
    meta_header = next(dss_meta_iterable)
    with GridWriter(
        grid_filename, meta_header.model_extent_name, meta_header.top_year_limit, meta_header.overall_limit
    ) as grid_writer:
        insert_meta_into_grid(grid_writer, meta_header, s3_client)
        for meta in dss_meta_iterable:
            insert_meta_into_grid(grid_writer, meta, s3_client)
    return grid_writer.parent_dir

def main(ms_client: Client, s3_client: Any, watershed_name: str, domain_name: str, top_by_year: int, zip_s3_uri: str):
    ranked_docs_iter = get_ranked_documents(ms_client, s3_client, watershed_name, domain_name, top_by_year)
    grid_fn = create_grid_filename(watershed_name)
    with TemporaryDirectory() as tmp_dir:
        full_directory_path = os.path.join(tmp_dir, watershed_name)
        grid_fn = create_grid_filename(watershed_name)
        write_meta_to_grid(full_directory_path, grid_fn, ranked_docs_iter, s3_client)
        zip_bucket, zip_key = split_s3_path(zip_s3_uri)
        zip_path = os.path.basename(zip_key).replace(".zip", "")
        logging.info(f"Zipping data to {zip_path}.zip")
        output_zip = make_archive(zip_path, "zip", tmp_dir, watershed_name)
        logging.info(f"Uploading file to {zip_s3_uri}")
        s3_client.upload_file(output_zip, zip_bucket, zip_key)

if __name__ == "__main__":
    import argparse

    import boto3

    parser = argparse.ArgumentParser()
    parser.add_argument("watershed_name", type=str, help="watershed name")
    parser.add_argument("domain_name", type=str, help="domain name")
    parser.add_argument("top_by_year", type=int, help="top storms by year")
    parser.add_argument("zip_s3_uri", type=str, help="s3 output zip for grid package")

    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], region_name=os.environ["AWS_REGION"])
    s3_client = session.client("s3")
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])

    args = parser.parse_args()

    main(ms_client, s3_client, args.watershed_name, args.domain_name, args.top_by_year, args.zip_s3_uri)
