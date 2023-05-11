""" Creates index used to store documents relating to SST processing """
import logging
import os

from constants import INDEX
from meilisearch import Client


def get_client():
    ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])
    return ms_client


def build_index(clean: bool = False, client: Client | None = None):
    """Builds index

    Args:
        clean (bool, optional):If True, tries to delete index with name specified in constants before creating. Defaults to False.
        client (Client | None, optional): Meilisearch client. Defaults to None.
    """
    if not client:
        client = get_client()
    if clean:
        delete_index(client)
    client.create_index(INDEX, {"primaryKey": "id"})


def assign_attributes(client: Client | None = None):
    """Assigns filterable and rankable attributes to built index

    Args:
        client (Client | None, optional): Meilisearch client. Defaults to None.
    """
    if not client:
        client = get_client()
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
        client (Client): Meilisearch client. Defaults to None.
    """
    if not client:
        ms_client = get_client()
    logging.warning(f"Deleting index {INDEX}")
    ms_client.index(INDEX).delete()


if __name__ == "__main__":
    import argparse

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

    client = get_client()

    if args.option == "build":
        build_index(args.clean, client)
    if args.option == "delete":
        delete_index(client)
