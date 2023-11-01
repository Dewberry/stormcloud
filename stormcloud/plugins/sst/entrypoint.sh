#!/bin/sh

echo 'Stochaistic storm transposition modeling plugin'
python -m main "{\"start_date\": \"2019-10-28\", \"hours_duration\": 72, \"watershed_name\": \"trinity\", \"watershed_uri\": \"s3://tempest/watersheds/trinity/trinity.geojson\", \"transposition_domain\": \"v00\", \"domain_uri\": \"s3://tempest/watersheds/trinity/trinity-transpo-area-v00.geojson\", \"s3_bucket\": \"tempest\", \"s3_prefix\": \"watersheds/trinity/trinity-transpo-area-v00/72h/\"}"