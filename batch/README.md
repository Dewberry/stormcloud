# Batch SST Jobs

## Summary

Scripts in this directory involve either copying logged information on SST batch job runs to an accessible location or submitting new SST batch jobs.

## Contents

### [batch-logs.py](batch-logs.py)
---
Script to collect and save batch logs from dss grid generation jobs to s3

### [send-job.py](send-job.py)
---
Script to handle submission of batch SST job using AWS resources to initiate SST analysis of a watershed over a specified period, saving summary statistics and DSS file of results to s3

#### Submitting SST Jobs

Inputs required for SST job submission include the watershed of interest, the transposition name of interest, the period of interest, the duration over which data should be summarized, and the URIs of s3 resources such as ATLAS14 data, the job definition, and the job queue to use in job. These inputs should be written in a JSON file, formatted as shown in [records](../records/sst/README.md)

#### Next Steps

After submitting a batch job to perform SST modeling for a watershed, you can monitor the progress of these batch jobs using the AWS console and check the logs using the batch-logs script detailed earlier.

To update the meilisearch database used to track information shown on the storm viewer website, take the JSON document you used to submit the batch job and use it as an input for the [meilisearch upload script](../ms/meilisearch_upload.py). More information on how to use this script can be found in the [meilisearch functions readme](../ms/README.md) or in the help documentation that is shown when running the meilisearch upload script with the command line parameter '-h' or '--help'. If the meilisearch database is not updated, results of the run will not be shown on the storm viewer site.
