FROM osgeo/gdal:ubuntu-small-3.5.1 as base
RUN apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install rasterio --no-binary rasterio
RUN apt-get install git
WORKDIR /plugin

# copy scripts and dependencies
COPY logger.py .

COPY write_zarr_to_dss.py .

# copy temp_precip_plugin scripts and dependencies
COPY temp_precip_plugin/requirements.txt .
COPY temp_precip_plugin/extract_storms_plugin_module.py .
COPY temp_precip_plugin/main.py .

# install python modules
RUN pip3 install -r requirements.txt

# install pydsstools from source
RUN apt-get install -y build-essential
RUN apt-get install -y gfortran
RUN apt-get install -y git
RUN apt-get install -y unzip
RUN git clone https://github.com/Dewberry/pydsstools.git

# download good heclib files
RUN mkdir heclib && curl https://www.hec.usace.army.mil/nexus/repository/maven-public/mil/army/usace/hec/heclib/7-IP-10-linux-x86_64/heclib-7-IP-10-linux-x86_64.zip --output heclib/heclib-7-IP-10-linux-x86_64.zip
RUN ( cd heclib && unzip heclib-7-IP-10-linux-x86_64.zip )

# replace corrupt heclib files
RUN rm -r pydsstools/pydsstools/src/external/dss/headers
RUN rm pydsstools/pydsstools/src/external/dss/linux64/heclib.a
RUN cp -a heclib/headers pydsstools/pydsstools/src/external/dss/headers
RUN cp -a heclib/heclib.a pydsstools/pydsstools/src/external/dss/linux64/heclib.a

# install
RUN ( cd pydsstools && python3 -m pip install . )

