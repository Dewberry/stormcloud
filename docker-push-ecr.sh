#!/bin/bash

set -euo pipefail

REPO=stormcloud

TAG=$1
AWS_REGION=$2
AWS_ACCOUNT_NUMBER=$3

REGISTRY=$AWS_ACCOUNT_NUMBER.dkr.ecr.$AWS_REGION.amazonaws.com
IMG=$REGISTRY/$REPO:$TAG

# build and push version and latest 
docker build . -t $IMG
docker push $IMG
docker tag $IMG $REGISTRY/$REPO:latest
docker push $REGISTRY/$REPO:latest