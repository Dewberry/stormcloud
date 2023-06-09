import datetime
import os
import pathlib
import shutil
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


def copy_dir(source_dir: str, copy_dest: str):
    print(f"Copying directory from {source_dir} to {copy_dest} prior to modification")
    return shutil.copytree(source_dir, copy_dest, dirs_exist_ok=True)


def proper_case_month(datetime_string: str) -> str:
    for month in MONTH_LIST:
        datetime_string = datetime_string.replace(month, month.capitalize())
    return datetime_string


def upper_case_month(datetime_string: str) -> str:
    for month in MONTH_LIST:
        datetime_string = datetime_string.replace(month.capitalize(), month)
    return datetime_string


def create_session(aws_access_key_id: str, aws_secret_access_key: str, region_name: str) -> object:
    print("Creating session")
    session = boto3.Session(
        aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name
    )
    s3 = session.resource("s3")
    return s3


def get_last_modification(s3: object, bucket: str, key: str) -> datetime.datetime:
    print(f"Getting metadata from s3://{bucket}/{key}")
    obj = s3.meta.client.head_object(Bucket=bucket, Key=key)
    last_modified = obj["LastModified"]
    return last_modified


def get_precip_pathnames(precip_dss_dir: str) -> List[Tuple[str, str]]:
    fn_pathname_list = []
    print(f"Getting pathnames from all DSS files in {precip_dss_dir}")
    dss_pathlib_dir = pathlib.Path(precip_dss_dir)
    for precip_dss_fn in dss_pathlib_dir.glob("*/*.dss"):
        precip_dss_fn = str(precip_dss_fn)
        with Open(precip_dss_fn) as f:
            for pathname in f.getPathnameList("/*/*/*/*/*/*/", sort=1):
                fn_pathname_list.append((precip_dss_fn, pathname))
    print(f"Total pathnames found: {len(fn_pathname_list)}")
    return fn_pathname_list


def insert_temperature_grid(
    temperature_dss_fn: str, precip_fn_pathname_list: List[Tuple[str, str]], dry: bool = False
) -> Generator[Tuple[str, str], None, None]:
    unique_dss_fns = []
    print(f"Opening source temperature DSS file {temperature_dss_fn}")
    with Open(temperature_dss_fn) as source_dss:
        for dest_dss_fn, pathname in precip_fn_pathname_list:
            # Format pathname to how it will be in the temperature DSS file
            pathname_parts = pathname.split("/")
            pathname_parts[1] = "SHG2K"
            pathname_parts[3] = "TEMPERATURE"
            begin_window = pathname_parts[4]
            begin_window_proper = proper_case_month(begin_window)
            begin_window_dt = datetime.datetime.strptime(begin_window_proper, "%d%b%Y:%H%M")
            if begin_window_dt.hour == 0:
                altered_window_dt = begin_window_dt - datetime.timedelta(hours=1)
                altered_window = upper_case_month(altered_window_dt.strftime("%d%b%Y:24%M"))
                pathname_parts[4] = altered_window
            pathname_parts[5] = ""
            temperature_pathname = "/".join(pathname_parts)
            source_dataset = source_dss.read_grid(temperature_pathname)
            with Open(dest_dss_fn) as dest_dss:
                # Format pathname to how we want it recorded (with hours from formatted and begin and end window indicated)
                pathname_parts[4] = begin_window
                end_window_dt = begin_window_dt + datetime.timedelta(hours=1)
                if end_window_dt.hour == 0:
                    end_window_dt -= datetime.timedelta(hours=1)
                    end_window = upper_case_month(end_window_dt.strftime("%d%b%Y:24%M"))
                else:
                    end_window = upper_case_month(end_window_dt.strftime("%d%b%Y:%H%M"))
                pathname_parts[5] = end_window
                temperature_pathname = "/".join(pathname_parts)
                try:
                    print(f"Inserting dataset at {temperature_pathname} in DSS file {dest_dss_fn}")
                    if not dry:
                        dest_dss.put_grid(temperature_pathname, source_dataset)
                    if dest_dss_fn not in unique_dss_fns:
                        yield temperature_pathname, dest_dss_fn
                        unique_dss_fns.append(dest_dss_fn)
                except AttributeError:
                    print(
                        f"Insert for {temperature_pathname} in DSS file {dest_dss_fn} failed. Datetime of failed insert: {begin_window_dt.isoformat()}"
                    )


def append_grid_record(
    grid_file_path: str,
    temperature_pathname: str,
    dss_filename: str,
    last_modification: datetime.datetime,
    dry: bool = False,
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
        print(f"Appending record for {temperature_pathname} to grid file {grid_file_path}")
        if not dry:
            grid_f.write(formatted_template)


if __name__ == "__main__":
    from dotenv import load_dotenv

    from pydsstools.heclib.utils import dss_logging

    dss_logging.config(level="None")

    load_dotenv()

    copy_dir("Kanawha-v01", "data-copy")

    s3 = create_session(
        os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"], os.environ["AWS_DEFAULT_REGION"]
    )

    temperature_last_modification = get_last_modification(s3, "tempest", "deliverables/kanawha-dss-cy.zip")
    precip_fn_pathnames = get_precip_pathnames("data-copy")
    for pathname, fn in insert_temperature_grid("kanwha-temp.dss", precip_fn_pathnames, dry=False):
        append_grid_record("data-copy/K_Transpose.grid", pathname, fn, temperature_last_modification, dry=False)
