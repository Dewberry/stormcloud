#!/bin/sh

echo ' '
echo 'Temp Precip Plugin:'
python -m plugin.main "{\"start_date\": \"1979-02-01\", \"end_date\": \"1979-03-01\", \"watershed_name\": \"Duwamish\", \"data_variables\": [\"APCP_surface\", \"TMP_2maboveground\"], \"zarr_s3_bucket\": \"tempest\", \"geojson_s3_path\": \"s3://tempest/watersheds/duwamish/duwamish.geojson\"}"

echo ' '
echo 'Plugin Metadata:'
cat /plugin/git-metadata.json