import boto3
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = boto3.client(
    "batch",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

JOB_DEF = "stormcloud-ec2:7"
JOB_QUEUE = "stormcloud-ec2-spot"

# arguments
duration = 72
watershed_name = "Indian Creek"
domain_name = "v01"
domain_uri = "s3://tempest/watersheds/indian-creek/indian-creek-transpo-area-v01.geojson"
watershed_uri = "s3://tempest/watersheds/indian-creek/indian-creek.geojson"
s3_bucket = "tempest"
s3_key_prefix = "watersheds/indian-creek/indian-creek-transpo-area-v01/72h/"
atlas_14_uri = "s3://tempest/transforms/atlas14/2yr03da/2yr03da.vrt"

# for POR
por_start = datetime(1979, 2, 1)
por_end = datetime(2022, 12, 31, 23)

# for single year
# year = 1979
# por_start = datetime(year, 2, 1)
# por_end = datetime(year, 12, 31) + timedelta(hours=72)


# Storm processing
process_start_time = datetime.now()
dt = por_start
yr = dt.year - 1
while dt + timedelta(hours=duration) <= por_end:

    start = dt.strftime("%Y%m%d")

    if dt.year != yr:
        print(dt.year)
        yr = dt.year

    cmd = [
        "python3",
        "extract_storms_v2.py",
        dt.strftime("%Y-%m-%d"),
        str(duration),
        watershed_name,
        domain_name,
        domain_uri,
        watershed_uri,
        s3_bucket,
        s3_key_prefix,
        atlas_14_uri,
    ]

    JOB_NAME = f"{watershed_name.replace(' ','')}-{domain_name}-{duration}h-{start}"

    response = client.submit_job(
        jobDefinition=JOB_DEF,
        jobName=JOB_NAME,
        jobQueue=JOB_QUEUE,
        containerOverrides={"command": cmd},
    )

    dt = dt + timedelta(hours=24)

print(process_start_time.strftime("%Y-%m-%d %H:%M"))
