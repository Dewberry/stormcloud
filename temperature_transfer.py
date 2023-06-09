import datetime
import os
import pathlib
from typing import Generator, List, Tuple

import boto3

from pydsstools.heclib.dss.HecDss import Open

GRID_RECORD_TEMPLATE = """Grid: AORC {top_date}
     Grid Type: {proper_type}
     Last Modified Date: {modification_date}
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
End:

"""

MONTH_LIST = ["JAN", "FEB", "MAR", "APR", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def proper_case_month(datetime_string: str):
    for month in MONTH_LIST:
        datetime_string = datetime_string.replace(month, month.capitalize())
    return datetime_string


def upper_case_month(datetime_string: str):
    for month in MONTH_LIST:
        datetime_string = datetime_string.replace(month.capitalize(), month)
    return datetime_string


def create_session(aws_access_key_id: str, aws_secret_access_key: str, region_name: str):
    """Creates s3 session using provided AWS credentials

    Args:
        aws_access_key_id (str): AWS Access Key ID
        aws_secret_access_key (str): AWS Secret Access Key
        region_name (str): AWS region

    Returns:
        Any: s3 resource
    """
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name
    )
    s3 = session.resource("s3")
    return s3


def get_last_modification(s3, bucket: str, key: str) -> datetime.datetime:
    obj = s3.meta.client.head_object(Bucket=bucket, Key=key)
    last_modified = obj["LastModified"]
    return last_modified


def get_precip_pathnames(precip_dss_dir: str) -> List[Tuple[str, str]]:
    fn_pathname_list = []
    dss_pathlib_dir = pathlib.Path(precip_dss_dir)
    for precip_dss_fn in dss_pathlib_dir.glob("*/*.dss"):
        precip_dss_fn = str(precip_dss_fn)
        with Open(precip_dss_fn) as f:
            for pathname in f.getPathnameList("/*/*/*/*/*/*/", sort=1):
                fn_pathname_list.append((precip_dss_fn, pathname))
    return fn_pathname_list


def insert_temperature_grid(
    temperature_dss_fn: str, precip_fn_pathname_list: List[Tuple[str, str]]
) -> Generator[Tuple[str, str], None, None]:
    with Open(temperature_dss_fn) as source_dss:
        for dest_dss_fn, pathname in precip_fn_pathname_list:
            pathname_parts = pathname.split("/")
            pathname_parts[1] = "SHG2K"
            pathname_parts[3] = "TEMPERATURE"
            begin_window = pathname_parts[4]
            begin_window_proper = proper_case_month(begin_window)
            begin_window_dt = datetime.datetime.strptime(begin_window_proper, "%d%b%Y:%H%M")
            if begin_window_dt.hour == 0:
                altered_window_dt = begin_window_dt - datetime.timedelta(hours=1)
                pathname_parts[4] = upper_case_month(f"{altered_window_dt.strftime('%d%b%Y:24%M')}")
            pathname_parts[5] = ""
            temperature_pathname = "/".join(pathname_parts)
            print(temperature_pathname)
            source_dataset = source_dss.read_grid(temperature_pathname)
            with Open(dest_dss_fn) as dest_dss:
                if begin_window_dt.hour == 0:
                    pathname_parts[4] = begin_window
                    temperature_pathname = "/".join(pathname_parts)
                print(f"Inserting dataset at {temperature_pathname} in DSS file {dest_dss_fn}")
                try:
                    dest_dss.put_grid(temperature_pathname, source_dataset, compute_range=True)
                    yield temperature_pathname, dest_dss_fn
                except AttributeError:
                    print(f"Insert failed, likely out of date range of temperature DSS file")


def append_grid_record(
    grid_file_path: str, temperature_pathname: str, dss_filename: str, last_modification: datetime.datetime
) -> None:
    with open(grid_file_path, "a") as grid_f:
        dss_datetime_str = os.path.basename(dss_filename).split("_")[0]
        dss_datetime = datetime.datetime.strptime(dss_datetime_str, "%Y%m%d")
        dss_basename = os.path.basename(dss_filename)
        formatted_template = GRID_RECORD_TEMPLATE.format(
            top_date=dss_datetime.strftime("%Y-%m-%d"),
            proper_type="Temperature",
            modification_date=last_modification.strftime("%d %B %Y"),
            modification_time=last_modification.strftime("%H:%M:%S"),
            relative_dss_fn=rf"\kanawha\dss\{dss_basename}",
            pathname=temperature_pathname,
        )
        print(f"writing\n{formatted_template}")
        grid_f.write(formatted_template)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from pydsstools.heclib.utils import dss_logging

    dss_logging.config(level="None")

    load_dotenv()

    s3 = create_session(
        os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ["AWS_DEFAULT_REGION"]
    )
    temperature_last_modification = get_last_modification(s3, "tempest", "deliverables/kanawha-dss-cy.zip")
    precip_fn_pathnames = get_precip_pathnames("Kanawha-v01")
    for pathname, fn in insert_temperature_grid("kanwha-temp.dss", precip_fn_pathnames):
        append_grid_record("Kanawha-v01/K_Transpose.grid", pathname, fn, temperature_last_modification)
