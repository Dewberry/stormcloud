import json
import logging
from types import NoneType
from typing import Any, Iterator, List, Union

from meilisearch import Client


def reconstruct_ranked_doc(ranked_doc_metadata: dict, s3_client: Any) -> dict:
    # load parent s3 doc
    parent_meta_uri = ranked_doc_metadata["parent_s3_uri"]
    bucket, *parts = parent_meta_uri.replace("s3://", "").split("/")
    key = "/".join(parts)
    res = s3_client.get_object(Bucket=bucket, Key=key)
    text = res.get("Body").read().decode()
    s3_data: dict = json.loads(text)
    reconstructed_dict = {}
    # add id
    reconstructed_dict["id"] = ranked_doc_metadata["id"]
    # copy start
    reconstructed_dict["start"] = s3_data["start"]
    # copy duration
    reconstructed_dict["duration"] = s3_data["duration"]
    # copy stats
    reconstructed_dict["stats"] = s3_data["stats"]
    # insert png url into metadata properties
    meta_copy = s3_data["metadata"].copy()
    meta_copy["png"] = ranked_doc_metadata["png_url"]
    reconstructed_dict["metadata"] = meta_copy
    # copy geom
    reconstructed_dict["geom"] = s3_data["geom"]
    # add ranks
    reconstructed_dict["ranks"] = ranked_doc_metadata["ranks"]
    # add categories
    reconstructed_dict["categories"] = ranked_doc_metadata["categories"]
    # if tropical storms attribute is in ranked_doc_metadat, put in
    ts = ranked_doc_metadata.get("tropical_storms")
    if ts != None:
        reconstructed_dict["tropical_storms"] = ts
    logging.info(f"successfully reconstructed meilisearch formatted document from ranked document metadata")
    return reconstructed_dict


def construct_key(
    json_key: Union[str, NoneType],
    watershed_name: Union[str, NoneType],
    transposition_domain_name: Union[str, NoneType],
    duration: Union[int, NoneType],
) -> str:
    def _sanitize(input_str: str) -> str:
        return input_str.replace(" ", "-").lower()

    if json_key:
        return json_key
    else:
        if watershed_name and transposition_domain_name and duration:
            assumed_json_key = f"watersheds/{_sanitize(watershed_name)}/{_sanitize(watershed_name)}-transpo-area-{_sanitize(transposition_domain_name)}/{duration}h/{_sanitize(watershed_name)}-{_sanitize(transposition_domain_name)}-ranked-events.json"
            return assumed_json_key
        raise TypeError(
            f"If no json key is provided, watershed name, transposition domain, and duration are required to guess JSON s3 key. One of these was missing: watershed_name: {watershed_name}, transposition_domain_name: {transposition_domain_name}, duration: {duration}"
        )


def get_ranked_docs(
    bucket: str,
    s3_client: Any,
    json_key: Union[str, NoneType] = None,
    watershed_name: Union[str, NoneType] = None,
    transposition_domain_name: Union[str, NoneType] = None,
    duration: Union[int, NoneType] = None,
) -> Iterator[dict]:
    json_key = construct_key(json_key, watershed_name, transposition_domain_name, duration)
    logging.info(f"getting ranked document information from s3://{bucket}/{json_key}")
    res = s3_client.get_object(Bucket=bucket, Key=json_key)
    text = res.get("Body").read().decode()
    data: List[dict] = json.loads(text)
    for d in data:
        yield d


class DocumentHandler:
    def __init__(self, client: Client, index: str, update: bool, batch_size: int = 100) -> None:
        self.client = client
        self.index = index
        self.batch_size = batch_size
        self.update = update
        self.queue: List[dict] = []

    def queue_document(self, doc: dict) -> None:
        logging.info(f"queueing document")
        if len(self.queue) < self.batch_size:
            logging.debug(f"adding doc to handler queue")
            self.queue.append(doc)
        else:
            self.handle_queue()
            self.queue = []

    def handle_queue(self) -> dict:
        logging.info("handling documents")
        if self.queue:
            if self.update:
                logging.info(f"updating meilisearch with docs -- sample: {self.queue[0]}")
                r = self.client.index(self.index).update_documents(self.queue)
            else:
                logging.info(f"adding to meilisearch with docs -- sample: {self.queue[0]}")
                r = self.client.index(self.index).add_documents(self.queue)
        else:
            logging.info(f"no documents left in queue")
            r = {}
        return r


if __name__ == "__main__":
    import argparse
    import os

    from dotenv import load_dotenv
    from client_utils import create_meilisearch_client, create_s3_client
    from constants import INDEX

    load_dotenv("stormcloud/ms/.env")

    logging.basicConfig(handlers=[logging.StreamHandler()], level=logging.INFO)

    ms_client = create_meilisearch_client(
        os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"]
    )

    s3_client = create_s3_client(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])

    common_args = argparse.ArgumentParser(add_help=False)
    common_args.add_argument(
        "--s3_bucket",
        type=str,
        required=False,
        default=None,
        help="s3 bucket to use when looking for ranked docs JSON; defaults to environment variable 'S3_BUCKET' if none provided",
    )
    common_args.add_argument(
        "--method",
        type=str,
        choices=["add", "update"],
        required=False,
        default="add",
        help="if add, adds documents in provided JSON as new meilisearch documents, failing if documents already exist with matching ids; if update, will overwrite existing documents with matching ids. Defaults to 'add'",
    )
    parser = argparse.ArgumentParser(add_help=True)
    subparsers = parser.add_subparsers(dest="subparser_name")
    json_parser = subparsers.add_parser("j", parents=[common_args])
    json_parser.add_argument("json_key", type=str, help="s3 key of JSON containing ranked document data")
    params_parser = subparsers.add_parser("p", parents=[common_args], add_help=False)
    params_parser.add_argument("watershed_name", type=str, help="watershed associated with ranked docs JSON")
    params_parser.add_argument(
        "transposition_domain_name",
        type=str,
        help="transposition domain name associated with ranked docs JSON",
    )
    params_parser.add_argument("duration", type=int, help="duration in hours associated with ranked docs JSON")

    args = parser.parse_args()
    args.update = args.method == "update"
    if not args.s3_bucket:
        args.s3_bucket = os.environ["S3_BUCKET"]

    doc_handler = DocumentHandler(ms_client, INDEX, args.update)

    if args.subparser_name == "j":
        for d in get_ranked_docs(args.s3_bucket, s3_client, args.json_key):
            d = reconstruct_ranked_doc(d, s3_client)
            doc_handler.queue_document(d)
    else:
        for d in get_ranked_docs(
            args.s3_bucket,
            s3_client,
            watershed_name=args.watershed_name,
            transposition_domain_name=args.transposition_domain_name,
            duration=args.duration,
        ):
            d = reconstruct_ranked_doc(d, s3_client)
            doc_handler.queue_document(d)
    doc_handler.handle_queue()
