import boto3
from datetime import datetime, timedelta
from dotenv import load_dotenv, find_dotenv
import json
from meilisearch import Client
import numpy as np
import os
from scipy.stats import rankdata

load_dotenv(find_dotenv())
session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
s3_client = session.client("s3")

s3_bucket = "tempest"

index_name = "events"
ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])

year_range = range(1979, 2023)

# watershed_domain = "kanawha/kanawha-transpo-area-v01"
# watershed_domain = "upper-green-1404/upper-green-transpo-area-v01"
# watershed_domain = "indian-creek/iowa-pilot"
watershed_domain = "indian-creek/indian-creek-transpo-area-v01"


# build meilisearch index
# ms_client.index(index_name).delete()
# ms_client.create_index(index_name, {"primaryKey": "start.timestamp"})
# ms_client.create_index(index_name, {"primaryKey": "id"})


docs = []
for year in year_range:
    print(year)
    s3_prefix = f"watersheds/{watershed_domain}/72h/docs/{year}"

    # get all s3 keys for year
    results = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    s3_keys = [content["Key"] for content in results["Contents"]]

    while "NextContinuationToken" in results.keys():
        results = s3_client.list_objects_v2(
            Bucket=s3_bucket, Prefix=s3_prefix, ContinuationToken=results["NextContinuationToken"]
        )
        s3_keys.extend([content["Key"] for content in results["Contents"]])

    # read in json files for year
    for key in s3_keys:
        result = s3_client.get_object(Bucket=s3_bucket, Key=key)
        doc = json.loads(result["Body"].read().decode())

        # add png url
        doc_meta = doc["metadata"]
        doc_meta[
            "png"
        ] = (
            png_url
        ) = f"https://tempest.s3.amazonaws.com/watersheds/{watershed_domain}/72h/pngs/{doc['start']['datetime'].split(' ')[0].replace('-', '')}.png"
        doc["metadata"] = doc_meta

        if watershed_domain == "indian-creek/iowa-pilot":
            doc_start = doc["start"]
            doc_start["timestamp"] = int(doc_start["timestamp"])
        doc["start"] = doc_start
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


# rank docs by year
docs = np.array(docs)
starts = np.array([datetime.strptime(d["start"]["datetime"], "%Y-%m-%d %H:%M:%S") for d in docs])
means = np.array([d["stats"]["mean"] for d in docs])
mean_ranks = rankdata(means * -1, method="ordinal")

# date decluster
for i in range(1, len(mean_ranks) + 1):
    idx = np.where(mean_ranks == i)
    dt = starts[idx][0]
    # print(i)
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
    print(year)
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

    ms_client.index(index_name).add_documents(ranked_docs)


# # filterable attributes
# ms_client.index(index_name).update_filterable_attributes(
#     [
#         "start.calendar_year",
#         "start.water_year",
#         "start.season",
#         "duration",
#         "metadata.source",
#         "metadata.watershed_name",
#         "metadata.transposition_domain_name",
#         "ranks.true_rank",
#         "ranks.declustered_rank",
#         "categories.lv10",
#         "categories.lv11",
#         "stats.mean",
#     ]
# )

# # sortable attributes
# ms_client.index(index_name).update_sortable_attributes(
#     ["start.timestamp", "stats.mean", "stats.max", "stats.norm_mean", "stats.sum"]
# )
