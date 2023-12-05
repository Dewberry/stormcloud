#!/bin/sh

echo 'Metadata standardization plugin for SST product pipeline'
python -m main "{\"watershed\": \"Duwamish\", \"extent_geojson_uri\": \"s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01.geojson\", \"start_date\": \"1994-10-26 00:00:00\"}"