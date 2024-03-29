#!/bin/sh

echo 'Temperature and precipitation DSS extraction plugin for AORC .zarr data'
python -m main "{\"start_date\": \"1979-02-02\", \"end_date\": \"1979-02-03\", \"watershed_name\": \"Trinity\", \"data_variables\": [\"APCP_surface\", \"TMP_2maboveground\"], \"zarr_s3_bucket\": \"tempest\", \"watershed_uri\": \"s3://tempest/watersheds/trinity/trinity-transpo-area-v01.geojson\", \"output_s3_bucket\": \"tempest\", \"output_s3_prefix\": \"watersheds/trinity/trinity-transpo-area-v01/test\"}"