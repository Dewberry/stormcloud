## AORC

A collection of scripts used to generate hourly CONUS AORC zarr files (temp or precip) from zip files located on NOAA FTP server.

#### Scripts:
* transfer - Script for transferring zipped AORC data from http server to s3 bucket.
* composite - Script to read in zipped files and create hourly CONUS-level composite gridded AORC datasets.
* constants - Constants used in AORC processing or parsing

##### Setup

 To run the scripts in this repo, you should have a .env file in the same directory as this repo which has the following keys:

 ```
AWS_ACCESS_KEY_ID=access_id_here
AWS_SECRET_ACCESS_KEY=access_key_here
AWS_DEFAULT_REGION=aws_region_here
 ```