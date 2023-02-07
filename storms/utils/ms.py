from dataclasses import dataclass
from dataclasses_json import dataclass_json
from meilisearch import Client
from typing import List


@dataclass_json
@dataclass
class __StormDocumentDateTime:
    datetime: str
    timestamp: int
    calendar_year: int
    water_year: int
    season: str


@dataclass_json
@dataclass
class __StormDocumentDuration:
    text: str
    integer: int
    units: str


@dataclass_json
@dataclass
class __StormDocumentStats:
    count: int
    mean: float
    max: float
    min: float
    sum: float
    norm_2yr: float


@dataclass_json
@dataclass
class __StormDocumentGeom:
    indexes: List[List[int]]
    center_x: float
    center_y: float
    area: float


@dataclass_json
@dataclass
class __StormDocumentMetaData:
    source: str
    watershed: str
    transposition_domain: str
    files: List[str]
    create_time: int


@dataclass_json
@dataclass
class __StormDocumentRanks:
    mean_rank: int
    max_rank: int
    norm_2yr_rank: int


@dataclass_json
@dataclass
class StormDocument:
    start: __StormDocumentDateTime
    duration: __StormDocumentDuration
    stats: __StormDocumentStats
    metadata: __StormDocumentMetaData
    geom: __StormDocumentGeom
    ranks: __StormDocumentRanks


def list_indexs(client: Client) -> List[str]:
    """
    Lists name of all indexes on the client

    Parameters
    ----------
    client: meilisearch.Client
        meilisearch Client

    Return
    ------
    List[str]
    """
    return [r.get("uid") for r in client.get_raw_indexes().get("results")]


def upload_docs(client: Client, index: str, docs: List[dict]):
    """
    Uploads a list of documents to a meilisearch index.

    Parameters
    ----------
    client: meilisearch.Client
        meilisearch Client
    index: str
        index name
    primary_key: str
        name of index's primary key (default "uid")
    docs: List[dict]
        list of documents to add to index
    """
    client.index(index).add_documents(docs)


def build_index(
    client: Client,
    index: str,
    primary_key: str = "uid",
    filterable_attributes: List[str] = None,
    sortable_attributes: List[str] = None,
):
    """
    Builds a meilisearch index if not already existing.

    Parameters
    ----------
    client: meilisearch.Client
        meilisearch Client
    index: str
        index name
    primary_key: str
        name of index's primary key (default "uid")
    filterable_attributes: List[str]
        names of filterable attributes
    sortable_attributes: List[str]
        names of sortable attributes
    """
    if not [r.get("uid") for r in client.get_raw_indexes().get("results") if r.get("uid") == index]:
        client.create_index(index, {"primaryKey": primary_key})

        if filterable_attributes:
            if filterable_attributes:
                client.index(index).update_filterable_attributes(filterable_attributes)

        if sortable_attributes:
            if sortable_attributes:
                client.index(index).update_sortable_attributes(sortable_attributes)

        # LOOK INTO DISTINCT ATTRIBUTE
        # LOOK INTO RANKING RULES
