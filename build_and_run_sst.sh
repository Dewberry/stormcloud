#!/bin/bash

docker build . -t sst-plugin -f plugins/sst/Dockerfile
docker run --rm -it --env-file plugins/sst/.env sst-plugin:latest