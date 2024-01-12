""" Creates index used to store documents relating to SST processing """
import logging

from constants import INDEX
from meilisearch import Client


def build_index(client: Client, clean: bool = False):
    """Builds index

    Args:
        clean (bool, optional):If True, tries to delete index with name specified in constants before creating. Defaults to False.
        client (Client): Meilisearch client
    """
    if clean:
        delete_index(client)
    client.create_index(INDEX, {"primaryKey": "id"})


def assign_attributes(client: Client):
    """Assigns filterable and rankable attributes to built index

    Args:
        client (Client): Meilisearch client
    """
    # filterable attributes
    client.index(INDEX).update_filterable_attributes(
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
    client.index(INDEX).update_sortable_attributes(
        ["start.timestamp", "stats.mean", "stats.max", "stats.norm_mean", "stats.sum"]
    )


def delete_index(client: Client):
    """Deletes index

    Args:
        client (Client): Meilisearch client
    """
    logging.warning(f"Deleting index {INDEX}")
    client.index(INDEX).delete()


if __name__ == "__main__":
    import argparse
    import os

    from dotenv import load_dotenv

    from ms.client_utils import create_meilisearch_client

    parser = argparse.ArgumentParser(
        prog="Meilisearch Init",
        description="Builds index given in constants.py",
        usage="Example usage: python ms/meilisearch-init.py build -c True",
    )

    parser.add_argument(
        "-o",
        "--option",
        type=str,
        required=True,
        choices=["build", "delete"],
        help="if build, builds index. if delete, deletes index",
    )
    parser.add_argument(
        "-c",
        "--clean",
        default=False,
        type=bool,
        required=False,
        help="if True, deletes existing index with given name before building index. If false, does not attempt delete before build. Defaults to False.",
    )

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    args = parser.parse_args()

    load_dotenv()

    ms_host = os.environ["REACT_APP_MEILI_HOST"]
    ms_api_key = os.environ["REACT_APP_MEILI_MASTER_KEY"]

    client = create_meilisearch_client(ms_host, ms_api_key)

    if args.option == "build":
        build_index(client, args.clean)
    if args.option == "delete":
        delete_index(client)
