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
    scale: float = 12.0
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

    @staticmethod
    def __format_name(name: str) -> str:
        cleaned = name.strip()
        lower = cleaned.lower()
        replaced = lower.replace(" ", "-")
        return replaced

    def get_job_name(self, dt: datetime) -> str:
        job_name = f"{self.watershed_name.replace(' ','')}-{self.domain_name}-{self.duration}h-{dt.strftime('%Y%m%d')}"
        return job_name


def load_inputs(json_path: str) -> JobInput:
    with open(json_path, "r") as f:
        data = json.load(f)
        if "por_start" in data.keys():
            data["por_start"] = datetime.strptime(data["por_start"], "%Y-%m-%d %H:%M")
        if "por_end" in data.keys():
            data["por_end"] = datetime.strptime(data["por_end"], "%Y-%m-%d %H:%M")
    return JobInput(**data)


def check_exists(keys: list[str], bucket: str, client) -> None:
    for key in keys:
        print(f"Checking {key}")
        logging.info(f"Checking {key}")
        client.head_object(Bucket=bucket, Key=key)


def construct_command(job_input: JobInput, current_dt: datetime) -> list[str]:
    cmd_list = [
        "python3",
        "extract_storms_v2.py",
        "-s",
        current_dt.strftime("%Y-%m-%d"),
        "-hr",
        str(job_input.duration),
        "-w",
        job_input.watershed_name,
        "-wu",
        job_input.watershed_uri,
        "-d",
        job_input.domain_name,
        "-du",
        job_input.domain_uri,
        "-b",
        job_input.s3_bucket,
        "-p",
        job_input.output_prefix,
        "-a",
        job_input.atlas_14_uri,
        "-sm",
        str(job_input.scale),
    ]
    return cmd_list


def send(job_input: JobInput) -> None:
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
    check_exists([job_input.domain_s3_key, job_input.watershed_s3_key], job_input.s3_bucket, s3_client)

    # Submission
    logging.info("Starting storm job submission")
    dt = job_input.por_start
    yr = dt.year - 1
    while dt + timedelta(hours=job_input.duration) <= job_input.por_end:
        if dt.year != yr:
            logging.info(f"Starting processing for year {dt.year}")
            yr = dt.year
        cmd_list = construct_command(job_input, dt)
        batch_client.submit_job(
            jobDefinition=job_input.job_def,
            jobName=job_input.get_job_name(dt),
            jobQueue=job_input.job_queue,
            containerOverrides={"command": cmd_list},
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
    print(send_inputs)
    send(send_inputs)
