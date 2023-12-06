import json
import logging
import os
from typing import Iterator, List

from common.cloud import split_s3_path
from common.dss import DSSProductMeta, decode_dss_meta_json
from common.grid import GridWriter, prepare_structure


def load_dss_product_metadata(s3_uris: List[str], s3_client) -> Iterator[DSSProductMeta]:
    for s3_uri in s3_uris:
        bucket, key = split_s3_path(s3_uri)
        result = s3_client.get_object(Bucket=bucket, Key=key)
        meta_data = result["Body"].read().decode("utf-8")
        meta_dict = json.loads(meta_data)
        meta_object = decode_dss_meta_json(meta_dict)
        yield meta_object


def write_meta_to_grid(grid_directory: str, grid_file_basename: str, s3_uris: List[str], s3_client) -> str:
    prepare_structure(grid_directory)
    grid_filename = os.path.join(grid_directory, grid_file_basename)
    meta_items = load_dss_product_metadata(s3_uris, s3_client)
    meta_header = next(meta_items)
    with GridWriter(
        grid_filename, meta_header.model_extent_name, meta_header.top_year_limit, meta_header.overall_limit
    ) as grid_writer:
        insert_meta_into_grid(grid_writer, meta_header, s3_client)
        for meta in meta_items:
            insert_meta_into_grid(grid_writer, meta, s3_client)
    return grid_writer.parent_dir


def insert_meta_into_grid(grid_writer: GridWriter, meta: DSSProductMeta, s3_client) -> None:
    dss_bucket, dss_key = split_s3_path(meta.dss_s3_uri)
    dss_basename = os.path.basename(dss_key)
    dss_fn = os.path.join(grid_writer.parent_dir, "dss", dss_basename)
    s3_client.download_file(dss_bucket, dss_key, dss_fn)
    relative_dss_fn = os.path.join("dss", dss_basename)
    for data_variable in meta.data_variables:
        sample_pathname = meta.sample_pathnames[data_variable.name]
        grid_writer.append_record(
            relative_dss_fn,
            sample_pathname,
            meta.last_modification,
            meta.rank_within_year,
            meta.overall_rank,
            meta.shg_x,
            meta.shg_y,
        )
