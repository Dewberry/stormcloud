""" Script for querying meilisearch database of SST model output """
from typing import List, Union

from meilisearch import Client


def query_ms(
    ms_client: Client,
    index: str,
    watershed_name: str,
    domain_name: str,
    mean_filter: float,
    limit: int,
    declustered: bool = True,
    top_by_year: Union[int, None] = None,
    filter_year: int = None,
) -> List[dict]:
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
    if filter_year:
        year_list = [f'start.calendar_year="{filter_year}"']
        filter_list.insert(0, year_list)
    query_params = {
        "filter": filter_list,
        "limit": limit,
        "sort": ["stats.mean:desc", "start.timestamp:asc"],
    }
    results = ms_client.index(index).search("", query_params)
    hits = results.get("hits")
    return hits


if __name__ == "__main__":
    import argparse
    import os

    from constants import INDEX
    from dotenv import load_dotenv

    from ms.client_utils import create_meilisearch_client

    load_dotenv()

    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    ms_client = create_meilisearch_client(ms_host, ms_api_key)

    parser = argparse.ArgumentParser(
        prog="SST Database Query",
        usage="python ms/storm_query 'Indian Creek' v01 'IC Transpose'",
        description="Executes query to retrieve storms with specified settings applied, then prints hits from database to console",
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
        "-d",
        "--declustered",
        default=True,
        type=bool,
        required=False,
        help="If true, use declustered rank to determine top storms by precipitation. If false, use true rank",
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

    hits = query_ms(
        ms_client,
        INDEX,
        args.watershed_name,
        args.domain_name,
        args.mean_filter,
        args.limit,
        args.declustered,
        args.top_by_year,
        args.filter_year,
    )

    for hit in hits:
        print(hit)
