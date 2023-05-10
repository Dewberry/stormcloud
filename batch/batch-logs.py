import boto3
from datetime import datetime
import json
import logging
import os
import enum
from typing import Any


class RunSetting(enum.Enum):
    LOCAL = enum.auto()
    BATCH = enum.auto()


def get_clients(run_setting: RunSetting) -> tuple[Any, Any, Any]:
    """Creates clients for interacting with AWS, depending on run environment setting

    Args:
        run_setting (RunSetting): Either LOCAL for local testing or BATCH for batch deployment

    Returns:
        tuple[Any, Any, Any]: Tuple containing s3 client, logs client, and batch client (in that order)
    """
    if run_setting == RunSetting.LOCAL:
        from dotenv import load_dotenv, find_dotenv

        # for local testing
        load_dotenv(find_dotenv())
        session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
        s3_client = session.client("s3")
        logs_client = session.client("logs")
        batch_client = session.client("batch")

    else:
        # for batch production
        from storms.utils import batch

        logging.getLogger("botocore").setLevel(logging.WARNING)
        os.environ.update(batch.get_secrets(secret_name="stormcloud-secrets", region_name="us-east-1"))
        session = boto3.session.Session()
        s3_client = session.client("s3")
        logs_client = session.client("logs", region_name="us-east-1")
        batch_client = session.client("batch")
    return s3_client, logs_client, batch_client


def get_batch_job_ids(batch_jobs_queue: str, job_name_filter: str, after_created_filter: int, batch_client) -> list:
    """Get all jobs from the job queue using the specified filters and extract the job ID."""
    batch_jobs_response = batch_client.list_jobs(
        jobQueue=batch_jobs_queue,
        filters=[
            {
                "name": "JOB_NAME",
                "values": [
                    job_name_filter,
                ],
            },
        ],
    )
    batch_jobs = [
        job["jobId"] for job in batch_jobs_response["jobSummaryList"] if job["createdAt"] >= after_created_filter
    ]
    while "nextToken" in batch_jobs_response.keys():
        batch_jobs_response = batch_client.list_jobs(
            jobQueue=batch_jobs_queue,
            filters=[
                {
                    "name": "JOB_NAME",
                    "values": [
                        job_name_filter,
                    ],
                },
            ],
            nextToken=batch_jobs_response["nextToken"],
        )
        batch_jobs.extend(
            [job["jobId"] for job in batch_jobs_response["jobSummaryList"] if job["createdAt"] >= after_created_filter]
        )

    return batch_jobs


def get_batch_job_statuses(
    batch_jobs_queue: str, job_name_filter: str, after_created_filter: int, batch_client
) -> list:
    """Get all jobs from the job queue using the specified filters and extract the job ID."""
    batch_jobs_response = batch_client.list_jobs(
        jobQueue=batch_jobs_queue,
        filters=[
            {
                "name": "JOB_NAME",
                "values": [
                    job_name_filter,
                ],
            },
        ],
    )
    batch_jobs = [
        job["status"] for job in batch_jobs_response["jobSummaryList"] if job["createdAt"] >= after_created_filter
    ]
    while "nextToken" in batch_jobs_response.keys():
        batch_jobs_response = batch_client.list_jobs(
            jobQueue=batch_jobs_queue,
            filters=[
                {
                    "name": "JOB_NAME",
                    "values": [
                        job_name_filter,
                    ],
                },
            ],
            nextToken=batch_jobs_response["nextToken"],
        )
        batch_jobs.extend(
            [job["status"] for job in batch_jobs_response["jobSummaryList"] if job["createdAt"] >= after_created_filter]
        )

    return batch_jobs


def describe_batch_jobs(job_ids: list, batch_client):
    if len(job_ids) <= 100:
        jobs = batch_client.describe_jobs(jobs=job_ids)["jobs"]

    else:
        jobs = []
        index = 0
        step = 100
        while index < len(job_ids):
            to_index = index + step
            if to_index > len(job_ids):
                to_index = len(job_ids)
            jobs.extend(batch_client.describe_jobs(jobs=job_ids[index:to_index])["jobs"])
            index = to_index

    return jobs


def get_logs(group_name: str, stream_name: str, logs_client):
    log_events_response = logs_client.get_log_events(
        logGroupName=group_name, logStreamName=stream_name, startFromHead=True
    )
    log_events = log_events_response["events"]
    next_token = ""
    while next_token != log_events_response["nextForwardToken"] and len(log_events) < 100:
        next_token = log_events_response["nextForwardToken"]
        log_events_response = logs_client.get_log_events(
            logGroupName=group_name, logStreamName=stream_name, nextToken=next_token, startFromHead=True
        )
        log_events.extend(log_events_response["events"])

    log_data = []
    for event in log_events:
        try:
            log_dict = json.loads(event["message"])
            if log_dict["level"] == "ERROR":
                log_data.append(log_dict)
        except:
            continue

    return log_data


def extract_job_logs(jobs: list, logs_client, log_group_name="/aws/batch/jobs"):
    n_successes = 0
    n_fails = 0
    n_other = 0
    fail_data = []

    for job in jobs:
        if job["status"] == "SUCCEEDED":
            n_successes += 1
        elif job["status"] == "FAILED":
            n_fails += 1

            attempts = []
            for attempt in job["attempts"]:
                log_stream = attempt["container"]["logStreamName"]
                log_data = get_logs(log_group_name, log_stream, logs_client)
                attempts.append(
                    {
                        "status_reason": attempt["statusReason"],
                        "log_stream_name": log_stream,
                        "logs": log_data,
                    }
                )

            fail_data.append(
                {
                    "name": job["jobName"],
                    "id": job["jobId"],
                    "status": job["status"],
                    "definition": job["jobDefinition"],
                    "attempts": attempts,
                }
            )
        else:
            n_other += 1

    return {
        "statuses": {
            "successes": n_successes,
            "fails": n_fails,
            "other": n_other,
            "total": n_successes + n_fails + n_other,
        },
        "fails": fail_data,
    }


def format_name(name: str) -> str:
    cleaned = name.strip()
    lower = cleaned.lower()
    replaced = lower.replace(" ", "-")
    return replaced


# main function
if __name__ == "__main__":
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format='{"time":"%(asctime)s", "level": "%(levelname)s", "message":%(message)s}',
        handlers=[logging.StreamHandler()],
    )

    parser = argparse.ArgumentParser(
        prog="Batch Log Collector",
        description="Collects and stores logs from AWS associated with SST model runs for a specific watershed",
        epilog="Example usage: python batch/batch-logs -c '2023-05-09 12:00' -w Duwamish -v V01 -s local ",
    )

    parser.add_argument(
        "-c",
        "--created_after",
        type=str,
        help="The datetime used to filter jobs searched for by the collector. Will only parse logs created after specified date. Date string expected in format YYYY-mm-dd HH:MM, like 2023-05-10 14:26",
        required=True,
    )
    parser.add_argument(
        "-w",
        "--watershed",
        type=str,
        help="Prefix used in filtering logs collected. In practice, watershed name for SST model of interest is used as filter",
        required=True,
    )
    parser.add_argument(
        "-v", "--domain_name", type=str, help="Domain name for transposition region used in SST model", required=True
    )
    parser.add_argument(
        "-d",
        "--hours_duration",
        default=72,
        type=int,
        help="Duration in hours used in SST model of interest",
        required=False,
    )
    parser.add_argument(
        "-s",
        "--setting",
        default="batch",
        type=str,
        choices=["batch", "local"],
        help="Specifies if script is being run in local development or in batch. Defaults to 'batch'",
        required=False,
    )
    parser.add_argument(
        "-q",
        "--job_queue",
        default="stormcloud-ec2-spot",
        type=str,
        help="aws job queue associated with logs. Defaults to 'stormcloud-ec2-spot'",
        required=False,
    )
    parser.add_argument(
        "-g",
        "--log_group",
        default="/aws/batch/job",
        type=str,
        help="aws log group associated with logs. Defaults to '/aws/batch/job'",
        required=False,
    )
    parser.add_argument(
        "-b",
        "--s3_bucket",
        default="tempest",
        type=str,
        help="s3 bucket in which parsed report on logs should be saved. Defaults to 'tempest'",
        required=False,
    )

    args = parser.parse_args()

    if args.setting == "local":
        s3_client, logs_client, batch_client = get_clients(RunSetting.LOCAL)
    else:
        s3_client, logs_client, batch_client = get_clients(RunSetting.BATCH)

    # Create job name filter using watershed name
    job_name_like = args.watershed + "*"

    # Convert created after filter to timestamp
    created_after_dt = datetime.strptime(args.created_after, "%Y-%m-%d %H:%M")
    created_after_timestamp = int(created_after_dt.timestamp() * 1000)

    # Format watershed and domain names
    watershed_name_formatted = format_name(args.watershed)
    domain_name_formatted = format_name(args.domain_name)

    # Create s3 key
    s3_key = f"watersheds/{watershed_name_formatted}/{watershed_name_formatted}-transpo-area-{domain_name_formatted}/{args.hours_duration}h/logs/{created_after_dt.strftime('%Y%m%d%H%M')}.json"

    logging.info(f"Getting logs created after {args.created_after} that correspond to jobs like {job_name_like}")

    job_ids = get_batch_job_ids(args.job_queue, job_name_like, created_after_timestamp, batch_client)
    jobs = describe_batch_jobs(job_ids, batch_client)
    job_logs = extract_job_logs(jobs, logs_client, args.log_group)
    s3_client.put_object(Bucket=args.s3_bucket, Key=s3_key, Body=json.dumps(job_logs))

    logging.info(f"Log report saved to s3://{args.s3_bucket}/{s3_key}")
