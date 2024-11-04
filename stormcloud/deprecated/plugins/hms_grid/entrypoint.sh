#!/bin/sh

echo 'GRID generation plugin for s3 DSS data'
python -m main "{\"metadata_s3_uris\": [\"s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01/standardized_metadata/19941026_19941029_SST_metadata.json\"], \"watershed\": \"Duwamish\", \"output_zip_s3_uri\": \"s3://tempest/deliverables/duwamish_hms_grid_test.zip\"}"