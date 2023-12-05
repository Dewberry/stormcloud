""" Utils which deal with writing GRID files referencing DSS files for precipitation or temperature data """
import datetime
import logging
import os
from io import TextIOWrapper
from typing import Tuple, Union

from .shared import DSSPathnameMeta, DSSVariable, decode_data_variable

GRID_FILE_HEADER_TEMPLATE = """Grid Manager: {initial} Transpose
     Version: 4.11
     Filepath Separator: \\
End:

"""

GRID_UNRANKED_RECORD_HEADER_TEMPLATE = """Grid: AORC {top_date}
     Grid Type: {grid_type}"""

GRID_YEARLY_RANKED_RECORD_HEADER_TEMPLATE = """Grid: AORC {top_date} Y{year_rank} T{overall_rank}
     Grid Type: {grid_type}"""

GRID_OVERALL_RANKED_RECORD_HEADER_TEMPLATE = """Grid: AORC {top_date} T{overall_rank}
     Grid Type: {grid_type}"""

GRID_RECORD_BODY_TEMPLATE = """     Last Modified Date: {modification_date}
     Last Modified Time: {modification_time}
     Reference Height Units: Meters
     Reference Height: 10.0
     Data Source Type: External DSS
     Variant: Variant-1
       Last Variant Modified Date: {modification_date}
       Last Variant Modified Time: {modification_time}
       Default Variant: Yes
       DSS File Name: {relative_dss_fn}
       DSS Pathname: {pathname}
     End Variant: Variant-1
     Use Lookup Table: No
"""

GRID_RECORD_STORM_BODY_TEMPLATE = (
    GRID_RECORD_BODY_TEMPLATE
    + """    Storm Center X: {storm_center_x}
     Storm Center Y: {storm_center_y}"""
)

GRID_RECORD_TEMPLATE = """{header}
{body}
End:

"""

MONTH_LIST = [
    "JAN",
    "FEB",
    "MAR",
    "APR",
    "JUN",
    "JUL",
    "AUG",
    "SEP",
    "OCT",
    "NOV",
    "DEC",
]


class GridWriter:
    def __init__(
        self,
        grid_filename: str,
        watershed: Union[str, None] = None,
        top_year_limit: int = 100,
        overall_limit: int = 1000,
        dry: bool = False,
    ) -> None:
        self.grid_filename = grid_filename
        self.watershed = watershed
        self.file = None
        self.dry = dry
        self.parent_dir = os.path.dirname(self.grid_filename)
        self.top_year_limit = top_year_limit
        self.overall_limit = overall_limit

    def __enter__(self):
        if not self.dry:
            self.file = self.prep_file()
        return self

    def __exit__(self, *args) -> None:
        args = list(filter(None, args))
        if not self.dry:
            self.file.close()
        if len(args) > 0:
            print(f"Grid writer exited with errors: {args}")

    def prep_file(self) -> Union[TextIOWrapper, None]:
        logging.info(f"Writing header to {self.grid_filename}")
        if not self.dry:
            f = open(self.grid_filename, "a")
            if os.path.exists(self.grid_filename):
                logging.warning(f"{self.grid_filename} already exists; not adding GRID header")
            else:
                formatted_header = GRID_FILE_HEADER_TEMPLATE.format(initial=self.watershed[0].upper())
                f.write(formatted_header)
            return f

    def append_record(
        self,
        dss_relative_path: str,
        pathname: str,
        last_modification: datetime.datetime,
        rank_within_year: Union[int, None] = None,
        rank_overall: Union[int, None] = None,
        storm_x: Union[float, None] = None,
        storm_y: Union[float, None] = None,
    ) -> None:
        logging.info(f"Appending record with pathname {pathname} to {self.grid_filename}")
        if not self.dry:
            full_dss_path = os.path.join(self.parent_dir, dss_relative_path)
            if not os.path.exists(full_dss_path):
                raise FileNotFoundError(
                    f"No dss file found at provided path {dss_relative_path} relative to grid parent directory {self.parent_dir}"
                )
            meta = decode_pathname(pathname)
            if meta.grid_variable == DSSVariable.TEMPERATURE:
                formatted_header_template = GRID_UNRANKED_RECORD_HEADER_TEMPLATE.format(
                    top_date=meta.start_dt.strftime("%Y-%m-%d"),
                    grid_type=meta.grid_variable.value,
                )
                formatted_body_template = GRID_RECORD_BODY_TEMPLATE.format(
                    modification_date=last_modification.strftime("%d %B %Y"),
                    modification_time=last_modification.strftime("%H:%M:%S"),
                    relative_dss_fn=dss_relative_path,
                    pathname=pathname,
                )
            else:
                if rank_within_year:
                    formatted_header_template = GRID_YEARLY_RANKED_RECORD_HEADER_TEMPLATE.format(
                        top_date=meta.start_dt.strftime("%Y-%m-%d"),
                        grid_type=meta.grid_variable.value,
                        year_rank=format_rank(rank_within_year, self.top_year_limit),
                        overall_rank=format_rank(rank_overall, self.overall_limit),
                    )
                else:
                    formatted_header_template = GRID_OVERALL_RANKED_RECORD_HEADER_TEMPLATE.format(
                        top_date=meta.start_dt.strftime("%Y-%m-%d"),
                        grid_type=meta.grid_variable.value,
                        overall_rank=format_rank(rank_overall, self.overall_limit),
                    )
                formatted_body_template = GRID_RECORD_STORM_BODY_TEMPLATE.format(
                    modification_date=last_modification.strftime("%d %B %Y"),
                    modification_time=last_modification.strftime("%H:%M:%S"),
                    relative_dss_fn=dss_relative_path,
                    pathname=pathname,
                    storm_center_x=storm_x,
                    storm_center_y=storm_y,
                )
            formatted_template = GRID_RECORD_TEMPLATE.format(
                header=formatted_header_template, body=formatted_body_template
            )
            self.file.write(formatted_template)


def prepare_structure(base_dir: str) -> None:
    dss_dir = os.path.join(base_dir, "dss")
    os.makedirs(dss_dir, exist_ok=True)


def format_rank(rank: int) -> str:
    limit_digits = len(str(limit))
    rank_str = str(rank).zfill(limit_digits)
    return rank_str


def decode_pathname(pathname: str) -> DSSPathnameMeta:
    """Decodes dss path name to yield metadata

    Args:
        pathname (str): dss pathname (ie /SHG4K/DUWAMISH/PRECIPITATION/06DEC2015:2400/07DEC2015:0100/AORC/) to split into

    Returns:
        DSSMeta: metadata extracted from path
    """
    _, shg, watershed, var_name, start_str, end_str, source, _ = pathname.split("/")
    resolution = int(shg[3])
    start_dt = handle_dss_date(start_str)
    grid_variable = decode_data_variable(var_name)
    if grid_variable == DSSVariable.PRECIPITATION:
        end_dt = handle_dss_date(end_str)
    else:
        end_dt = None
    meta = DSSPathnameMeta(resolution, watershed, start_dt, end_dt, source, grid_variable)
    return meta


def handle_dss_date(dss_dt_str: str) -> datetime.datetime:
    for month in MONTH_LIST:
        dss_dt_str = dss_dt_str.replace(month.capitalize(), month)
    if not dss_dt_str.endswith("2400"):
        dss_dt = datetime.datetime.strptime(dss_dt_str, "%d%b%Y:%H%M")
    else:
        dss_dt = datetime.datetime.strptime(dss_dt_str, "%d%b%Y:2400")
        dss_dt += datetime.timedelta(days=1)
    return dss_dt
