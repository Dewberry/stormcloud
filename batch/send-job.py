""" Takes JSON file input and submits batch jobs for SST processing based on input data """
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List

import boto3


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
    """Create JobInput class instance from JSON file input

    Args:
        json_path (str): Path to JSON file

    Returns:
        JobInput: Cleaned JSON data
    """
    with open(json_path, "r") as f:
        data = json.load(f)
        if "por_start" in data.keys():
            data["por_start"] = datetime.strptime(data["por_start"], "%Y-%m-%d %H:%M")
        if "por_end" in data.keys():
            data["por_end"] = datetime.strptime(data["por_end"], "%Y-%m-%d %H:%M")
    return JobInput(**data)


def check_exists(keys: List[str], bucket: str, client) -> None:
    """Check if keys exist

    Args:
        keys (List[str]): List of s3 keys
        bucket (str): Bucket holding keys
        client: s3 client
    """
    for key in keys:
        logging.info(f"Checking {key}")
        client.head_object(Bucket=bucket, Key=key)


def construct_command(job_input: JobInput, current_dt: datetime) -> List[str]:
    """Constructs list of commands from job input for submission to batch. Commands should be valid for extract_storms_v2.py

    Args:
        job_input (JobInput): Job inputs
        current_dt (datetime): Datetime of interest for batch job

    Returns:
        List[str]: list of commands
    """
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
    """Sends jobs over period of record for watershed and transposition region specified in input

    Args:
        job_input (JobInput): Batch job parameters
    """
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
    import argparse

    from dotenv import find_dotenv, load_dotenv

    load_dotenv(find_dotenv())

    parser = argparse.ArgumentParser(
        prog="Batch Job Submitter",
        description="Submits SST batch jobs based on environment variables and input JSON document",
        usage="Example: python batch/send-job.py -f records/duwamish.json",
    )
    parser.add_argument(
        "-f",
        "--filepath",
        type=str,
        required=True,
        help="Path to JSON file with SST job parameters. See records/README.md for expected format.",
    )

    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    if not os.path.exists(args.filepath):
        raise FileExistsError(f"Input JSON file does not exist: {args.filepath}")

    send_inputs = load_inputs(args.filepath)
    logging.info(f"Starting send process with inputs: {send_inputs}")
    send(send_inputs)
