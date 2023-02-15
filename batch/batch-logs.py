import boto3
from datetime import datetime
import json
import logging
import os
import sys


# # for local testing
# from dotenv import load_dotenv, find_dotenv

# load_dotenv(find_dotenv())
# session = boto3.session.Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])
# s3_client = session.client("s3")
# logs_client = session.client("logs")
# batch_client = session.client("batch")

# for batch production
from storms.utils import batch

logging.getLogger("botocore").setLevel(logging.WARNING)
os.environ.update(batch.get_secrets(secret_name="stormcloud-secrets", region_name="us-east-1"))
session = boto3.session.Session()
s3_client = session.client("s3")
logs_client = session.client("logs")
batch_client = session.client("batch")


def get_batch_job_ids(
    batch_jobs_queue: str,
    job_name_filter: str,
    after_created_filter: int,
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


def describe_batch_jobs(job_ids):

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


def get_logs(group_name, stream_name):
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
            pass

    return log_data


def extract_job_logs(jobs, log_group_name="/aws/batch/jobs"):
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
                log_data = get_logs(log_group_name, log_stream)
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


# main function
if __name__ == "__main__":
    args = sys.argv

    created_after = args[1]
    job_name_like = args[2]
    job_queue = args[3]
    log_group_name = args[4]
    s3_bucket = args[5]
    s3_key = args[6]

    created_after_timestamp = int(datetime.strptime(created_after, "%Y-%m-%d %H:%M").timestamp() * 1000)
    job_ids = get_batch_job_ids(job_queue, job_name_like, created_after_timestamp)

    jobs = describe_batch_jobs(job_ids)

    job_logs = extract_job_logs(jobs, log_group_name)

    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=json.dumps(job_logs))
