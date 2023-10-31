#!/bin/bash

python -m temp_precip_plugin.get_git_info
docker build -f ./temp_precip_plugin/Dockerfile -t temp-precip-plugin
docker run --rm -it temp-precip-plugin:latest