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
elif [[ $PLUGIN == "ms_update" ]]; then
    echo building $PLUGIN
    docker build . -t ms-update-plugin -f Dockerfile.ms_update
    docker run --rm -it --env-file stormcloud/plugins/ms_update/.env ms-update-plugin:latest
else
  echo "plugin arg must be one of [sst, temp_precip, ms_update]"
fi

