#!/bin/bash

docker build . -t temp-precip-plugin -f plugins/temp_precip/Dockerfile
docker run --rm -it --env-file plugins/temp_precip/.env temp-precip-plugin:latest