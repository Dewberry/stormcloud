#!/bin/bash

docker build -t sst-plugin -f Dockerfile.sst_plugin .
docker run --rm -it sst-plugin \
  python -m main "{\"s3_bucket\":\"tempest\" , \"s3_prefix\":\"watersheds\" , \"start_date\": \"2023-10-27\", \"hours_duration\": \"72\", \"watershed_name\": \"trinity\", \"watershed_uri\": \"s3://tempest/watersheds/trinity/trinity.geojson\", \"domain_name\": \"v00\", \"domain_uri\": \"s3://tempest/watersheds/trinity/trinity-transpo-area-v00.geojson\"}"
