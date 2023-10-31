#!/bin/bash

python -m sst_plugin.get_git_info
docker build -f ./sst_plugin/Dockerfile -t sst-plugin
docker run --rm -it sst-plugin:latest