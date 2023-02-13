import boto3
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

client = boto3.client(
    "batch",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
)

JOB_DEF = "stormcloud-ec2:6"
JOB_QUEUE = "stormcloud-ec2"

start = "1979-02-02"
duration = 72
watershed_name = "Upper Green"
domain_name = "V01"
domain_uri = "s3://tempest/watersheds/upper-green-1404/upper-green-transpo-area-v01.geojson"
watershed_uri = "s3://tempest/watersheds/upper-green-1404/upper-green-1404.geojson"
s3_bucket = "tempest"
s3_key_prefix = "watersheds/upper-green-1404/upper-green-transpo-area-v01/72h"

cmd = [
    "python3",
    "extract_storms_v2.py",
    start,
    str(duration),
    watershed_name,
    domain_name,
    domain_uri,
    watershed_uri,
    s3_bucket,
    s3_key_prefix,
]

# cmd = ["printenv"]

JOB_NAME = f"{watershed_name.replace(' ','')}-{domain_name}-{duration}hr-{start.replace('-','')}"

response = client.submit_job(
    jobDefinition=JOB_DEF,
    jobName=JOB_NAME,
    jobQueue=JOB_QUEUE,
    containerOverrides={"command": cmd},
)
