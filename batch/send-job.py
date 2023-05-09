import boto3
import os
import logging
import json
from datetime import datetime, timedelta
from dataclasses import dataclass, field


@dataclass
class JobInput:
    watershed_name: str
    domain_name: str
    duration: int
    atlas_14_uri: str
    job_def: str
    job_queue: str
    s3_bucket: str = "tempest"
    por_start: datetime = datetime(1979, 2, 1, 0)
    por_end: datetime = datetime(2022, 12, 31, 23)
    watershed_uri: str = field(init=False)
    watershed_s3_key: str = field(init=False)
    domain_uri: str = field(init=False)
    domain_s3_key: str = field(init=False)
    output_prefix: str = field(init=False)

    def __post_init__(self):
        formatted_watershed_name = self.__format_name(self.watershed_name)
        formatted_domain_name = self.__format_name(self.domain_name)

        self.domain_s3_key = f"watersheds/{formatted_watershed_name}/{formatted_watershed_name}-transpo-area-{formatted_domain_name}.geojson"
        self.domain_uri = f"s3://{self.s3_bucket}/{self.domain_s3_key}"

        self.watershed_s3_key = f"watersheds/{formatted_watershed_name}/{formatted_watershed_name}.geojson"
        self.watershed_uri = f"s3://{self.s3_bucket}/{self.watershed_s3_key}"
        self.output_prefix = f"watersheds/{formatted_watershed_name}/{formatted_watershed_name}-transpo-area-{formatted_domain_name}/{self.duration}h"

        self.job_name = f"{self.watershed_name.replace(' ','')}-{self.domain_name}-{self.duration}h-{self.por_start.strftime('%Y%m%d')}"

    @staticmethod
    def __format_name(name: str) -> str:
        cleaned = name.strip()
        lower = cleaned.lower()
        replaced = lower.replace(" ", "-")
        return replaced


def load_inputs(json_path: str) -> JobInput:
    with open(json_path, "r") as f:
        data = json.load(f)
        data["por_start"] = datetime.strptime(data["por_start"], "%Y-%m-%d %H:%M")
        data["por_end"] = datetime.strptime(data["por_end"], "%Y-%m-%d %H:%M")
    return JobInput(**data)


def check_exists(keys: list[str], bucket: str, client) -> None:
    for key in keys:
        print(f"Checking {key}")
        logging.info(f"Checking {key}")
        client.head_object(Bucket=bucket, Key=key)


def main(inputs: JobInput) -> None:
    # Create batch client
    batch_client = boto3.client(
        service_name="batch",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Create s3 client
    s3_client = boto3.client(
        service_name="s3",
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    )

    # Make sure that domain and watershed paths created exist and throw error if not
    check_exists([inputs.domain_s3_key, inputs.watershed_s3_key], inputs.s3_bucket, s3_client)

    # Submission
    logging.info("Starting storm job submission")
    dt = inputs.por_start
    yr = dt.year - 1
    while dt + timedelta(hours=inputs.duration) <= inputs.por_end:
        if dt.year != yr:
            logging.info(f"Starting processing for year {dt.year}")
            yr = dt.year
        cmd = [
            "python3",
            "extract_storms_v2.py",
            dt.strftime("%Y-%m-%d"),
            str(inputs.duration),
            inputs.watershed_name,
            inputs.domain_name,
            inputs.domain_uri,
            inputs.watershed_uri,
            inputs.s3_bucket,
            inputs.output_prefix,
            inputs.atlas_14_uri,
        ]
        batch_client.submit_job(
            jobDefinition=inputs.job_def,
            jobName=inputs.job_name,
            jobQueue=inputs.job_queue,
            containerOverrides={"command": cmd},
        )

        dt = dt + timedelta(hours=24)

    logging.info("Submission finished")


if __name__ == "__main__":
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv())

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    send_inputs = load_inputs("records/duwamish.json")
    main(send_inputs)
