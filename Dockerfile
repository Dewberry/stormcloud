FROM osgeo/gdal:ubuntu-small-latest as base

RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install rasterio --no-binary rasterio

ADD storms /app/storms/
ADD whls /app/whls/

COPY requirements.txt /app/requirements.txt

WORKDIR /app

RUN pip3 install -r requirements.txt 

RUN pip3 install /app/whls/*.whl