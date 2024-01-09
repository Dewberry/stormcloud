import json

from meilisearch import Client


def main(client: Client, index: str, doc_json_fn: str, update: bool) -> None:
    with open(doc_json_fn) as f:
        docs = json.load(f)
    if not isinstance(docs, list):
        raise TypeError(f"Basic validation of checking if documents are a list failed")
    if update:
        client.index(index).update_documents(docs)
    else:
        client.index(index).add_documents(docs)


if __name__ == "__main__":
    import argparse
    import os

    from client_utils import create_meilisearch_client
    from constants import INDEX

    client = create_meilisearch_client(
        os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"]
    )

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "method",
        type=str,
        choices=["add", "update"],
        default="add",
        help="if add, adds documents in provided JSON as new meilisearch documents, failing if documents already exist with matching ids; if update, will overwrite existing documents with matching ids. Defaults to 'add'",
    )
    parser.add_argument(
        "documents_json_fn",
        type=str,
        help="local filename of JSON file containing a list of meilisearch documents for upload",
    )
