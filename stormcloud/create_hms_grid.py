import os
from typing import Iterator, List
from common.grid import GridWriter, DSSPathnameMeta, prepare_structure
from common.dss import DSSProductMeta, decode_dss_meta_json
from common.cloud import split_s3_path


def load_dss_product_metadata(s3_uris: List[str], s3_client) -> Iterator[DSSProductMeta]:
    for s3_uri in s3_uris:
        bucket, key = split_s3_path(s3_uri)
        result = s3_client.get_object(Bucket=bucket, Key=key)
        meta_dict = result["Body"].read().decode("utf-8")
        meta_object = decode_dss_meta_json(meta_dict)
        yield meta_object


def write_meta_to_grid(grid_directory: str, grid_file_basename: str, s3_uris: List[str], s3_client) -> str:
    prepare_structure(grid_directory)
    grid_filename = os.path.join(grid_directory, grid_file_basename)
    # TODO: Make the year limit legit
    # TODO: Make the overall limit legit
    with GridWriter(grid_filename, meta.watershed, meta.top_year_limit, meta.overall_limit) as grid_writer:
        for meta in load_dss_product_metadata(s3_uris, s3_client):
            dss_bucket, dss_key = split_s3_path(meta.dss_s3_uri)
            dss_basename = os.path.basename(dss_key)
            dss_fn = os.path.join(grid_directory, "dss", dss_basename)
            s3_client.download_file(dss_bucket, dss_key, dss_fn)
            relative_dss_fn = os.path.join("dss", dss_basename)
            # TODO: Make the sample pathname legit
            grid_writer.append_record(
                relative_dss_fn,
                meta.sample_pathname,
                meta.last_modification,
                meta.rank_within_year,
                meta.overall_rank,
                meta.shg_x,
                meta.shg_y,
            )
