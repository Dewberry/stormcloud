#!/bin/bash

docker build . -t temp-precip-plugin -f plugins/temp_precip/Dockerfile
docker run --rm -it temp-precip-plugin:latest