FROM osgeo/gdal:ubuntu-small-3.6.3
RUN apt-get update && apt-get -y upgrade
RUN apt-get install -y python3-pip && pip3 install --upgrade pip

COPY requirements.txt /app/requirements.txt
WORKDIR /app
RUN pip3 install -r requirements.txt

# install pydsstools from source
RUN apt-get install -y build-essential gfortran git unzip

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
# RUN ( cd pydsstools && python3 -m pip wheel . )
RUN ( cd pydsstools && python3 -m pip install . )


COPY ./test /app/test
COPY transposition.py /app/transposition.py