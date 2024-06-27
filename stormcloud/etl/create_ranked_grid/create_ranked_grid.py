"""
ETL to query meilisearch for top storm (per year or overall) for a watershed and transposition domain, then format the metadata into the expected format for hms grid creation, then created a zipped grid package and upload to s3
"""

from __future__ import annotations

import datetime
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
from common.cloud import get_last_modification, split_s3_path, check_if_exists
from common.dss import DSSProductMeta
from common.shared import DSSVariable, NOAADataVariable
from write_aorc_zarr_to_dss import SpecifiedInterval, generate_dss_from_zarr


def create_grid_filename(watershed: str) -> str:
    initials = ""
    for char in watershed:
        if char.isupper():
            initials += char
    return f"{initials}_Transpose.grid"


def create_transform(tgt_epsg: str = "EPSG:4326") -> Callable:
    wgs84 = pyproj.CRS(tgt_epsg)
    transform_function = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform
    return transform_function


def get_ranked_documents_precip_only(
    ms_client: Client,
    s3_client: Any,
    watershed_name: str,
    domain_name: str,
    top_by_year: int | None,
    limit: int,
    mean_filter: float,
    declustered: bool,
) -> Iterator[DSSProductMeta]:
    docs = query_ms(ms_client, INDEX, watershed_name, domain_name, mean_filter, limit, declustered, top_by_year)
    for i, doc in enumerate(docs, 1):
        if top_by_year != None:
            if declustered:
                year_rank = doc["ranks"]["declustered_rank"]
            else:
                year_rank = doc["ranks"]["true_rank"]
        else:
            year_rank = None
        dss_uri = guess_dss_uri(
            doc["metadata"]["transposition_domain_source"], doc["start"]["datetime"], doc["duration"]
        )
        transform_function = create_transform()
        meta = construct_dss_meta(
            doc["metadata"]["watershed_name"],
            doc["metadata"]["transposition_domain_source"],
            dss_uri,
            doc["start"]["datetime"],
            None,
            None,
            doc["duration"],
            doc["geom"]["center_x"],
            doc["geom"]["center_y"],
            i,
            year_rank,
            limit,
            top_by_year,
            s3_client,
            transform_function=transform_function,
        )
        yield meta


def get_ranked_documents_temp_precip(
    ms_client: Client,
    s3_client: Any,
    watershed_name: str,
    domain_name: str,
    top_by_year: int | None,
    limit: int,
    mean_filter: float,
    declustered: bool,
    zarr_bucket: str,
    s3_output_bucket: str,
    access_key_id: str,
    secret_access_key: str,
    output_resolution_km: int,
) -> Iterator[DSSProductMeta]:
    docs = query_ms(ms_client, INDEX, watershed_name, domain_name, mean_filter, limit, declustered, top_by_year)
    transform_function = create_transform()
    for i, doc in enumerate(docs, 1):
        if top_by_year != None:
            if declustered:
                year_rank = doc["ranks"]["declustered_rank"]
            else:
                year_rank = doc["ranks"]["true_rank"]
        else:
            year_rank = None
        with TemporaryDirectory() as tmp_dir:
            interval = SpecifiedInterval.WEEK
            # adjust start date by 1 hour in order to make first precipitation record start at 12 am to 1 am instead of including precipitation from the previous day (11 pm to 12 am)
            start_dt = datetime.datetime.fromisoformat(doc["start"]["datetime"]) + datetime.timedelta(hours=1)
            end_dt = start_dt + datetime.timedelta(hours=doc["duration"])
            geojson_bucket, geojson_key = split_s3_path(doc["metadata"]["transposition_domain_source"])
            dss_basename = f'{doc["metadata"]["watershed_name"].lower()}_{start_dt.strftime("%Y%m%d")}_{end_dt.strftime("%Y%m%d")}.dss'
            dss_s3_key = os.path.join(geojson_key.replace(".geojson", ""), "with_temp", dss_basename)
            # skip creation of dss if it already exists on s3
            if not check_if_exists(s3_client, s3_output_bucket, dss_s3_key):
                logging.info(f"Generating DSS for data from {start_dt.isoformat()} to {end_dt.isoformat()}")
                dss_paths = [
                    p
                    for p in generate_dss_from_zarr(
                        tmp_dir,
                        doc["metadata"]["watershed_name"],
                        start_dt,
                        end_dt,
                        [NOAADataVariable.APCP, NOAADataVariable.TMP],
                        zarr_bucket,
                        geojson_bucket,
                        geojson_key,
                        access_key_id,
                        secret_access_key,
                        interval,
                        output_resolution_km,
                    )
                ]
                if len(dss_paths) != 1:
                    raise ValueError(f"Expected 1 DSS file to be generated; got {len(dss_paths)}")
                else:
                    dss_path = dss_paths[0]
                # save dss to s3
                logging.info(f"Uploading DSS data to s3://{s3_output_bucket}/{dss_s3_key}")
                s3_client.upload_file(dss_path, s3_output_bucket, dss_s3_key)
                upload_dt = datetime.datetime.now()
            else:
                logging.info(f"{dss_s3_key} already exists as s3 key; skipping creation")
                upload_dt = get_last_modification(s3_client, s3_output_bucket, dss_s3_key)
            dss_uri = f"s3://{s3_output_bucket}/{dss_s3_key}"

            # construct and save metadata to s3
            dss_vars = [DSSVariable.PRECIPITATION, DSSVariable.TEMPERATURE]
            meta = construct_dss_meta(
                doc["metadata"]["watershed_name"],
                f"s3://{geojson_bucket}/{geojson_key}",
                dss_uri,
                doc["start"]["datetime"],
                None,
                upload_dt,
                doc["duration"],
                doc["geom"]["center_x"],
                doc["geom"]["center_y"],
                i,
                year_rank,
                limit,
                top_by_year,
                s3_client,
                [d.name for d in dss_vars],
                transform_function,
            )
            yield meta


def write_meta_to_grid(
    grid_directory: str, grid_file_basename: str, dss_meta_iterable: Iterable[DSSProductMeta], s3_client: Any
) -> str:
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


def main(
    ms_client: Client,
    s3_client: Any,
    watershed_name: str,
    domain_name: str,
    top_by_year: int | None,
    limit: int,
    mean_limit: float,
    declustered: bool,
    zip_s3_uri: str,
    with_temp: bool,
    zarr_bucket: str | None,
    s3_output_bucket: str | None,
    access_key_id: str | None,
    secret_access_key: str | None,
    output_resolution_km: int | None,
):
    if with_temp:
        if not all(
            [
                zarr_bucket,
                s3_output_bucket,
                access_key_id,
                secret_access_key,
                output_resolution_km,
            ]
        ):
            reqs = [
                "zarr_bucket",
                "s3_output_bucket",
                "access_key_id",
                "secret_access_key",
                "output_resolution_km",
            ]
            raise ValueError(
                f"Missing required inputs; when with_temp is set to true, the following become required: {reqs}"
            )
        ranked_docs_iter = get_ranked_documents_temp_precip(
            ms_client,
            s3_client,
            watershed_name,
            domain_name,
            top_by_year,
            limit,
            mean_limit,
            declustered,
            zarr_bucket,
            s3_output_bucket,
            access_key_id,
            secret_access_key,
            output_resolution_km,
        )
    else:
        ranked_docs_iter = get_ranked_documents_precip_only(
            ms_client, s3_client, watershed_name, domain_name, top_by_year, limit, mean_limit, declustered
        )
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
    parser.add_argument("zip_s3_uri", type=str, help="s3 output zip for grid package")

    parser.add_argument(
        "--top_by_year",
        type=int,
        required=False,
        default=None,
        help="if creating package of top n storms per year, provide n storms; defaults to None",
    )
    parser.add_argument(
        "--limit",
        type=int,
        required=False,
        default=1000,
        help="if creating package of top storms overall, provide overall number of storms to collect; if used in conjunction with 'top_by_year', it simply sets a limit on storms retrieved; defaults to 1000 (max documents retrievable from meilisearch in a single query)",
    )
    parser.add_argument(
        "--mean_limit",
        type=float,
        required=False,
        default=0,
        help="minimum mean filter to apply when searching for storms; defaults to 0",
    )
    parser.add_argument(
        "--declustered",
        action="store_true",
        help="if declustered, storms will be filtered to limit storms selected to at most 1 storm from the same 72 hour window; defaults to False",
    )
    parser.add_argument(
        "--with_temp",
        action="store_true",
        help="if DSS files are to be recreated and saved to s3 with temperature included rather than using the DSS files created by the SST process, provide this flag, else DSS files from SST process will be used by default; defaults to False",
    )
    parser.add_argument(
        "--zarr_bucket",
        type=str,
        required=False,
        default="tempest",
        help="s3 bucket to search for .zarr data to convert to DSS format; only required if with_temp is set to True; defaults to 'tempest'",
    )
    parser.add_argument(
        "--s3_output_bucket",
        type=str,
        required=False,
        default="tempest",
        help="s3 bucket to which DSS files will be written; only required if with_temp is set to True; defaults to 'tempest'",
    )
    parser.add_argument(
        "--output_resolution_km",
        type=int,
        required=False,
        default=1,
        help="resolution in kilometers to use when converting .zarr data to DSS format; only required if with_temp is set to True; defaults to 1",
    )

    session = boto3.session.Session(
        os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], region_name=os.environ["AWS_REGION"]
    )

    s3_client = session.client("s3")
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])

    args = parser.parse_args()

    logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO)
    botocore_logger = logging.getLogger("botocore")
    botocore_logger.setLevel(logging.WARNING)

    main(
        ms_client,
        s3_client,
        args.watershed_name,
        args.domain_name,
        args.top_by_year,
        args.limit,
        args.mean_limit,
        args.declustered,
        args.zip_s3_uri,
        args.with_temp,
        args.zarr_bucket,
        args.s3_output_bucket,
        os.environ["AWS_ACCESS_KEY_ID"],
        os.environ["AWS_SECRET_ACCESS_KEY"],
        args.output_resolution_km,
    )
