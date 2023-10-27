#!/bin/sh

echo ' '
echo 'SST Plugin:'
python -m plugin.main "{\"start_date\": \"2023-10-27\", \"hours_duration\": \"72\", \"watershed_name\": \"duwamish\", \"watershed_uri\": \"s3://tempest/watersheds/duwamish/duwamish.geojson\", \"domain_name\": \"v01\", \"domain_uri\": \"s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01.geojson\"}"

echo ' '
echo 'Plugin Metadata:'
cat /plugin/git-metadata.json