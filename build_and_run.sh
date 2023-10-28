#!/bin/bash

python -m plugin.get_git_info
docker build -f ./plugin/Dockerfile -t sst-plugin
docker run --rm -it sst-plugin:latest