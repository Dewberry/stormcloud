import datetime
import logging
from typing import Generator, Tuple

from meilisearch import Client

from .constants import INDEX
from .storm_query import query_ms


def get_time_windows(
    year: int,
    watershed_name: str,
    domain_name: str,
    n: int,
    declustered: bool,
    ms_client: Client,
) -> Generator[Tuple[datetime.datetime, datetime.datetime, int], None, None]:
    """Generates start and end time windows for storms identified in meilisearch database as in the top storms when ranked by mean precipitation

    Args:
        year (int): Year of interest
        watershed_name (str): Watershed of interest
        domain_name (str): Transposition domain of interest
        n (int): Number of top storms from which time windows should be pulled
        declustered (bool): If true, use declustered rank which ensures that the duration of SST models do not overlap when ranking by mean precipitation. If false, use unfiltered rank by mean precipitation.
        ms_client (Client): Client used to query meilisearch database for SST model run information

    Yields:
        Generator[Tuple[datetime.datetime, datetime.datetime, int], None, None]: Yields tuple of start time, end time pulled from storm, and storm mean precipitation rank, respectively
    """
    if declustered:
        search_method_name = "declustered"
    else:
        search_method_name = "true"
    logging.info(
        f"Finding time windows aligning with top {n} storms in year {year} for watershed {watershed_name}, transposition region version {domain_name} using {search_method_name} rank"
    )
    for hit in query_ms(
        ms_client, INDEX, watershed_name, domain_name, 0, n, declustered, n, year
    ):
        rank = hit["ranks"]["true_rank"]
        if declustered:
            rank = hit["ranks"]["declustered_rank"]
        start_dt_str = hit["start"]["datetime"]
        duration = hit["duration"]
        start_dt = datetime.datetime.fromisoformat(start_dt_str)
        end_dt = start_dt + datetime.timedelta(hours=duration)
        yield start_dt, end_dt, rank
