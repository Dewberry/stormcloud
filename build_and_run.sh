#!/bin/bash

PLUGIN=$1

if [[ $PLUGIN == "sst" ]]; then
    echo building $PLUGIN
    docker build . -t sst-plugin -f Dockerfile.sst
    docker run --rm -it --env-file stormcloud/plugins/sst/.env sst-plugin:latest
elif [[ $PLUGIN == "temp_precip" ]]; then
    echo building $PLUGIN
    docker build . -t temp-precip-plugin -f Dockerfile.temp_precip
    docker run --rm -it --env-file stormcloud/plugins/temp_precip/.env temp-precip-plugin:latest
elif [[ $PLUGIN == "hms_grid" ]]; then
    echo building $PLUGIN
    docker build . -t hms-grid-plugin -f Dockerfile.hms_grid
    docker run --rm -it --env-file stormcloud/plugins/hms_grid/.env hms-grid-plugin:latest
elif [[ $PLUGIN == "standardize_meta" ]]; then
    echo building $PLUGIN
    docker build . -t standardize-meta-plugin -f Dockerfile.standardize_meta
    docker run --rm -it --env-file stormcloud/plugins/standardize_meta/.env standardize-meta-plugin:latest
else
  echo "plugin arg must be one of [sst, temp_precip, identify_sst_results, hms_grid, standardize_meta]"
fi

