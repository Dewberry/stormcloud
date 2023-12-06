# Meilisearch Functions

## Summary

The scripts in this directory are to be used for triggering updates to or querying information from the meilisearch database holding statistics and s3 URIs relating to SST model runs

## Contents

### [client_utils.py](client_utils.py)

---
Common utilities used by multiple meilisearch scripts for creating AWS clients for interacting with meilisearch as s3 resources

### [constants.py](constants.py)

---
Constants used to consistently reference the same meilisearch database

### [meilisearch_init.py](meilisearch_init.py)

---
Script used to create the index, or 'database', in meilisearch used to track storm data. This should not be necessary to use, but is included for traceability and provenance. For more information on the usage of this script or its parameters, view the help information associated with the script by launching the script in python with the command line parameter '-h' or '--help' provided

### [meilisearch_upload.py](meilisearch_upload.py)

---
Script used to upload s3 SST model run data to the meilisearch database. This data is ranked as it is ingested according to the mean precipitation and will be viewable on the storm viewer web application. This script can also be used to update existing meilisearch documents, replacing specified attributes in the matching document. This matching is done using an ID calculated using the watershed name, the transposition domain name, the duration, and the start date of the period of interest.

Be cautious when using either function because updating information cannot be undone and uploads can potentially fail if performed for already existing data.

For more information on the usage of this script or its parameters, view the help information associated with the script by launching the script in python with the command line parameter '-h' or '--help' provided

### [storm_query.py](storm_query.py)

---
Script used to query the meilisearch database for storms of interest using specified filters or rankings. For more information on the usage of this script or its parameters, view the help information associated with the script by launching the script in python with the command line parameter '-h' or '--help' provided
