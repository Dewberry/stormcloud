#!/bin/sh

echo 'StormViewer ranked doc creator'
python -m main "{\"watershed_name\": \"trinity\", \"transposition_domain\": \"v01\", \"duration\": 72, \"s3_bucket\": \"tempest\", \"tropical_storm_json_s3_uri\": \"s3://tempest/watersheds/trinity/trinity_ibtracs.json\"}"