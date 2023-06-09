""" Script to update meilisearch index with new documents or edit existing document attributes """
import json
import logging
import os
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import numpy as np
from constants import INDEX
from dotenv import find_dotenv, load_dotenv
from scipy.stats import rankdata
from ms.client_utils import create_meilisearch_client, create_s3_client


@dataclass
class MeilisearchInputs:
    watershed_name: str
    domain_name: str
    s3_bucket: str = "tempest"
    start_year: int = 1979
    end_year: int = 2022

    @staticmethod
    def __format_name(name: str) -> str:
        cleaned = name.strip()
        lower = cleaned.lower()
        replaced = lower.replace(" ", "-")
        return replaced

    @property
    def full_name(self):
        return f"{self.__format_name(self.watershed_name)}/{self.__format_name(self.watershed_name)}-transpo-area-{self.__format_name(self.domain_name)}"


def load_inputs(json_path: str) -> MeilisearchInputs:
    with open(json_path) as f:
        data = json.load(f)
        selection = {"watershed_name": data["watershed_name"], "domain_name": data["domain_name"]}
        if "s3_bucket" in data.keys():
            selection["s3_bucket"] = data["s3_bucket"]
        if "por_start" in data.keys():
            selection["start_year"] = datetime.strptime(data["por_start"], "%Y-%m-%d %H:%M").year
        if "por_end" in data.keys():
            selection["end_year"] = datetime.strptime(data["por_end"], "%Y-%m-%d %H:%M").year
    inputs = MeilisearchInputs(**selection)
    return inputs


def get_keys(client: object, bucket: str, prefix: str) -> Generator[str, None, None]:
    """Gets keys from a bucket

    Args:
        client: s3 client
        bucket (str): s3 bucket
        prefix (str): prefix to use in filtering keys

    Yields:
        Generator[str, None, None]: Yields keys
    """
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        contents = page.get("Contents", [])
        for content in contents:
            key = content.get("Key")
            yield key


def structure_document(input_data: dict) -> dict:
    """Structures document for upload / update functions

    Args:
        input_data (dict): Data parsed from s3 SST docs

    Returns:
        dict: Data with additional attributes "id", "png", and "categories"
    """
    # Get and format watershed and transposition region names
    meta = input_data.get("metadata")
    watershed_name = meta.get("watershed_name")
    domain_name = meta.get("transposition_domain_name")
    watershed_name_formatted = watershed_name.lower().replace(" ", "-")
    domain_name_formatted = domain_name.lower()

    # Add png url to metadata
    data_datetime = input_data.get("start").get("datetime").split()[0]
    data_datetime_formatted = data_datetime.replace("-", "")
    meta[
        "png"
    ] = f"https://tempest.s3.amazonaws.com/watersheds/{watershed_name_formatted}/{watershed_name_formatted}-transpo-area-{domain_name_formatted}/72h/pngs/{data_datetime_formatted}"
    input_data["metadata"] = meta

    # Add categories
    watershed_name = meta.get("watershed_name")
    domain_name = meta.get("transposition_domain_name")
    categories = {
        "lv10": watershed_name,
        "lv11": f"{watershed_name} > {domain_name}",
    }
    input_data["categories"] = categories

    # Add id to serve as primary key
    data_id = (
        f"{watershed_name_formatted}_{domain_name_formatted}_{input_data.get('duration')}h_{data_datetime_formatted}"
    )
    input_data["id"] = data_id
    return input_data


def rank_documents(data: List[dict], year_range: range) -> List[dict]:
    """Ranks documents based on mean precipitation values creates separate rank which eliminates values within 71 hour window of high means using mask

    Args:
        data (list[dict]): Unranked data dictionaries containing mean data and start times
        year_range (range): Range of years to use in partitioning data

    Returns:
        list[dict]: Ranked data dictionaries
    """
    logging.info("Start ranking")
    # rank docs by year
    docs = np.array(data)
    starts = np.array([datetime.strptime(d["start"]["datetime"], "%Y-%m-%d %H:%M:%S") for d in docs])
    means = np.array([d["stats"]["mean"] for d in docs])
    mean_ranks = rankdata(means * -1, method="ordinal")

    # date decluster
    for i in range(1, len(mean_ranks) + 1):
        idx = np.where(mean_ranks == i)
        dt = starts[idx][0]
        if i == 1:
            decluster_mask = np.array([True])
            starts_by_mean = np.array([dt])
        else:
            min_dt = dt - timedelta(hours=71)
            max_dt = dt + timedelta(hours=71)
            if np.any((starts_by_mean[decluster_mask] >= min_dt) & (starts_by_mean[decluster_mask] <= max_dt)):
                decluster = False
            else:
                decluster = True
            decluster_mask = np.append(decluster_mask, decluster)
            starts_by_mean = np.append(starts_by_mean, dt)
    years = np.array([dt.year for dt in starts])
    years_by_mean = np.array([dt.year for dt in starts_by_mean])

    # get true ranks and declustered rank for each date
    for year in year_range:
        yr_decluster_mask = decluster_mask[years_by_mean == year]
        yr_starts_by_mean = starts_by_mean[years_by_mean == year]
        yr_starts = starts[years == year]
        yr_docs = docs[years == year]
        ranked_docs = []
        for doc, start in zip(yr_docs, yr_starts):
            idx = np.where(yr_starts_by_mean == start)[0][0]
            if yr_decluster_mask[idx]:
                decluster_rank = np.where(yr_starts_by_mean[yr_decluster_mask] == start)[0][0] + 1
            else:
                decluster_rank = -1
            true_rank = idx + 1
            doc["ranks"] = {
                "true_rank": int(true_rank),
                "declustered_rank": int(decluster_rank),
            }
            ranked_docs.append(doc)
    return ranked_docs


def upload(
    inputs: MeilisearchInputs, access_key_id: str, secret_access_key: str, ms_host: str, ms_api_key: str
) -> None:
    s3_client = create_s3_client(access_key_id, secret_access_key)
    ms_client = create_meilisearch_client(ms_host, ms_api_key)
    year_range = range(inputs.start_year, inputs.end_year + 1)
    docs = []
    for year in year_range:
        logging.info(f"Gathering docs for {year}")
        s3_prefix = f"watersheds/{inputs.full_name}/72h/docs/{year}"

        # read in json files for year
        for key in get_keys(s3_client, inputs.s3_bucket, s3_prefix):
            result = s3_client.get_object(Bucket=inputs.s3_bucket, Key=key)
            doc = json.load(result.get("Body"))
            restructured = structure_document(doc)
            docs.append(restructured)

    # rank documents
    ranked_docs = rank_documents(docs, year_range)
    ms_client.index(INDEX).add_documents(ranked_docs)


def update(
    inputs: MeilisearchInputs,
    update_attribute: str,
    access_key_id: str,
    secret_access_key: str,
    ms_host: str,
    ms_api_key: str,
):
    s3_client = create_s3_client(access_key_id, secret_access_key)
    ms_client = create_meilisearch_client(ms_host, ms_api_key)
    year_range = range(inputs.start_year, inputs.end_year + 1)
    docs = []
    for year in year_range:
        logging.info(f"Gathering docs for {year}")
        s3_prefix = f"watersheds/{inputs.full_name}/72h/docs/{year}"

        # read in json files for year
        for key in get_keys(s3_client, inputs.s3_bucket, s3_prefix):
            result = s3_client.get_object(Bucket=inputs.s3_bucket, Key=key)
            doc = json.load(result.get("Body"))
            restructured = structure_document(doc)
            if update_attribute not in restructured.keys():
                raise ValueError(
                    f"Expected one of the following attributes: {restructured.keys()}; got {update_attribute}"
                )
            if ms_client.index(INDEX).get_document(restructured["id"]):
                docs.append({"id": restructured["id"], update_attribute: restructured[update_attribute]})
            else:
                raise ValueError(
                    f"Document id parsed for update does not exist in the database. ID searched: {restructured['id']}"
                )
    ms_client.index(INDEX).update_documents(docs)


if __name__ == "__main__":
    import argparse

    from dotenv import find_dotenv, load_dotenv

    parser = argparse.ArgumentParser(
        prog="Meilisearch Updater",
        description="Script to upload new documents or edit existing documents for SST models based on model output documentation on s3",
        usage="python ms/meilisearch-upload.py -f records/duwamish.json -o update -a metadata",
    )

    parser.add_argument(
        "-f", "--filepath", type=str, required=True, help="JSON document which defines watershed of interest"
    )

    parser.add_argument(
        "-o",
        "--option",
        type=str,
        required=True,
        choices=["upload", "update"],
        help="If upload, uploads docs parsed from s3. If update, should be accompanied with attribute to update and will update specified attribute of parsed docs.",
    )
    parser.add_argument(
        "-a",
        "--attribute",
        default=None,
        type=str,
        required=False,
        help="Attribute of parsed document to use in meilisearch update",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    load_dotenv(find_dotenv())

    access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    if not os.path.exists(args.filepath):
        raise FileExistsError(f"No JSON document exists at {args.filepath}")
    ms_inputs = load_inputs(args.filepath)

    if args.option == "upload":
        logging.info(f"Proceeding with upload based on inputs: {ms_inputs}")
        upload(ms_inputs, access_key_id, secret_access_key, ms_host, ms_api_key)

    if args.option == "update":
        if not args.attribute:
            raise ValueError("Update option given but no attribute to update provided")
        logging.info(f"Updating attribute {args.attribute} based on inputs: {ms_inputs}")
        update(ms_inputs, args.attribute, access_key_id, secret_access_key, ms_host, ms_api_key)
