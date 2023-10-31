#!/bin/bash

docker build . -t sst-plugin -f plugins/sst/Dockerfile
docker run --rm -it sst-plugin:latest