import numpy as np
import os
import boto3
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
from meilisearch import Client
from scipy.stats import rankdata
from dataclasses import dataclass
from constants import INDEX
import pickle


@dataclass
class MeilisearchInputs:
    watershed_name: str
    domain_name: str
    s3_bucket: str = "tempest"
    start_year: int = 1979
    end_year: int = 2023

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


def update_documents(inputs: MeilisearchInputs) -> None:
    session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
    s3_client = session.client("s3")
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])

    year_range = range(inputs.start_year, inputs.end_year + 1)

    docs = []
    for year in year_range:
        logging.info(f"Gathering docs for {year}")
        s3_prefix = f"watersheds/{inputs.full_name}/72h/docs/{year}"

        # get all s3 keys for year
        results = s3_client.list_objects_v2(Bucket=inputs.s3_bucket, Prefix=s3_prefix)
        s3_keys = [content["Key"] for content in results["Contents"]]

        while "NextContinuationToken" in results.keys():
            results = s3_client.list_objects_v2(
                Bucket=inputs.s3_bucket, Prefix=s3_prefix, ContinuationToken=results["NextContinuationToken"]
            )
            s3_keys.extend([content["Key"] for content in results["Contents"]])

        # read in json files for year
        for key in s3_keys:
            result = s3_client.get_object(Bucket=inputs.s3_bucket, Key=key)
            doc = json.loads(result["Body"].read().decode())

            # add png url
            doc_meta = doc["metadata"]
            doc_meta[
                "png"
            ] = f"https://tempest.s3.amazonaws.com/watersheds/{inputs.full_name}/72h/pngs/{doc['start']['datetime'].split(' ')[0].replace('-', '')}.png"
            doc["metadata"] = doc_meta
            doc_cats = {
                "lv10": doc_meta["watershed_name"],
                "lv11": f"{doc_meta['watershed_name']} > {doc_meta['transposition_domain_name']}",
            }
            doc["categories"] = doc_cats

            # add id
            doc[
                "id"
            ] = f'{doc["metadata"]["watershed_name"].lower().replace(" ","-")}_{doc["metadata"]["transposition_domain_name"].lower()}_{doc["duration"]}h_{doc["start"]["datetime"].split(" ")[0].replace("-", "")}'

            docs.append(doc)

    logging.info("Start ranking")
    # rank docs by year
    docs = np.array(docs)
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

    # is there a way I can give rank for everything and then have a decluster dates check box
    # essentially use two different ranks values
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
            # Reassign norm mean to None if np.nan value
            if doc["stats"]["norm_mean"]:
                if np.isnan(doc["stats"]["norm_mean"]):
                    doc["stats"]["norm_mean"] = None
            ranked_docs.append(doc)
        ms_client.index(INDEX).add_documents(ranked_docs)


def build_index():
    # build meilisearch index
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])
    ms_client.index(INDEX).delete()
    ms_client.create_index(INDEX, {"primaryKey": "start.timestamp"})
    ms_client.create_index(INDEX, {"primaryKey": "id"})


def assign_attributes():
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])
    # filterable attributes
    ms_client.index(INDEX).update_filterable_attributes(
        [
            "start.calendar_year",
            "start.water_year",
            "start.season",
            "duration",
            "metadata.source",
            "metadata.watershed_name",
            "metadata.transposition_domain_name",
            "ranks.true_rank",
            "ranks.declustered_rank",
            "categories.lv10",
            "categories.lv11",
            "stats.mean",
        ]
    )

    # sortable attributes
    ms_client.index(INDEX).update_sortable_attributes(
        ["start.timestamp", "stats.mean", "stats.max", "stats.norm_mean", "stats.sum"]
    )


if __name__ == "__main__":
    from dotenv import find_dotenv, load_dotenv

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    load_dotenv(find_dotenv())
    ms_inputs = load_inputs("records/duwamish.json")
    update_documents(ms_inputs)
