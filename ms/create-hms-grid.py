import json
import os
from datetime import datetime

import boto3
import pyproj
from meilisearch import Client
from pydsstools.heclib.dss import HecDss
from pydsstools.heclib.utils import SHG_WKT
from shapely.geometry import Point
from shapely.ops import transform

session = boto3.session.Session()
bucket_name = os.environ["S3_BUCKET_NAME"]
s3_client = session.client("s3")


ms_client = Client(os.environ["REACT_APP_MEILI_HOST"], api_key=os.environ["REACT_APP_MEILI_MASTER_KEY"])
index_name = "events"

data_dir = "IndianCreek"
grid_file = os.path.join(data_dir, "IC_Transpose.grid")
dss_dir = os.path.join(data_dir, "dss")

wgs84 = pyproj.CRS("EPSG:4326")
project = pyproj.Transformer.from_crs(wgs84, SHG_WKT, always_xy=True).transform


docs = ms_client.index(index_name).search(
    "",
    {
        "filter": [
            f'stats.mean >= 3.491 AND ranks.declustered_rank >= 1 AND metadata.watershed_name = "Indian Creek" AND metadata.transposition_domain_name = "v01"'
        ],
        "limit": 500,
        "sort": ["stats.mean:desc", "start.timestamp:asc"],
    },
)["hits"]


# make grid file
with open(grid_file, "w") as gridf:
    gridf.write("Grid Manager: IC Transpose\n")
    gridf.write("     Version: 4.11\n")
    gridf.write("     Filepath Separator: \\\n")
    gridf.write("End:\n\n")

    for i, doc in enumerate(docs):
        s3_uri = os.path.join(
            doc["metadata"]["transposition_domain_source"].replace(".geojson", ""),
            f"{doc['duration']}h",
            "dss",
            f"{doc['id'].split('_')[-1]}.dss",
        )
        s3_key = s3_uri.replace(f"s3://{bucket_name}/", "")
        dss_name = f"{doc['id'].split('_')[-1]}_Y{doc['ranks']['declustered_rank']:03}_T{i+1:03}.dss"
        dss_path = os.path.join(dss_dir, dss_name)

        s3_client.download_file(bucket_name, s3_key, dss_path)

        with HecDss.Open(dss_path) as dss:
            dss_pathname = dss.getPathnameList("/*/*/*/*/*/*/", sort=1)[0]

        dss_filename = os.path.join("C:/Data", dss_path).replace("/", "\\")

        last_modified = s3_client.head_object(Bucket=bucket_name, Key=s3_key)["LastModified"]
        last_modified_date = datetime.strftime(last_modified, "%d %B %Y")
        last_modified_time = datetime.strftime(last_modified, "%H:%M:%S")

        # transform center to SHG
        wgs_point = Point(doc["geom"]["center_x"], doc["geom"]["center_y"])
        shg_point = transform(project, wgs_point)

        # get grid info
        grid = f"{doc['metadata']['source']} {doc['start']['datetime'].split(' ')[0]} Y{doc['ranks']['declustered_rank']:03} T{i+1:03}"
        grid_type = "Precipitation"
        ref_height_units = "Meters"
        ref_height = "10.0"
        data_source_type = "External DSS"
        variant = "Variant-1"
        default_variant = "Yes"
        end_variant = "Variant-1"
        use_lookup_table = "No"
        storm_center_x = shg_point.x
        storm_center_y = shg_point.y

        # write grid info
        gridf.write(f"Grid: {grid}\n")

        gridf.write(f"     Grid Type: {grid_type}\n")
        gridf.write(f"     Last Modified Date: {last_modified_date}\n")
        gridf.write(f"     Last Modified Time: {last_modified_time}\n")
        gridf.write(f"     Reference Height Units: {ref_height_units}\n")
        gridf.write(f"     Reference Height: {ref_height}\n")
        gridf.write(f"     Data Source Type: {data_source_type}\n")
        gridf.write(f"     Variant: {variant}\n")

        gridf.write(f"       Last Variant Modified Date: {last_modified_date}\n")
        gridf.write(f"       Last Variant Modified Time: {last_modified_time}\n")
        gridf.write(f"       Default Variant: {default_variant}\n")
        gridf.write(f"       DSS File Name: {dss_filename}\n")
        gridf.write(f"       DSS Pathname: {dss_pathname}\n")

        gridf.write(f"     End Variant: {end_variant}\n")
        gridf.write(f"     Use Lookup Table: {use_lookup_table}\n")
        gridf.write(f"     Storm Center X: {storm_center_x}\n")
        gridf.write(f"     Storm Center Y: {storm_center_y}\n")

        gridf.write(f"End:\n\n")
