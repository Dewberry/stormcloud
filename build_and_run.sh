#!/bin/bash

PLUGIN=$1

if [[ $PLUGIN == "sst" ]]; then  
    echo building $PLUGIN 
    docker build . -t sst-plugin -f Dockerfile.sst
    docker run --rm -it --env-file plugins/sst/.env sst-plugin:latest
elif [[ $PLUGIN == "temp_precip" ]]; then  
    echo building $PLUGIN 
    docker build . -t temp-precip-plugin -f Dockerfile.temp_precip
    docker run --rm -it --env-file plugins/temp_precip/.env temp-precip-plugin:latest
else
  echo "plugin arg must be one of [sst, temp_precip]"
fi 

