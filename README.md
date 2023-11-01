# stormcloud
Creates datasets for hydrologic modeling in the cloud.

---

[Workflow](workflow.md)

---

[Meilisearch](stormcloud/ms/README.md)

---

## Adding Temperature Data

[Write AORC data for precipitation or temperature to DSS format](stormcloud/write_aorc_zarr_to_dss.py)

 - Requires use of docker container as specified in [Dockerfile.temp_precip](Dockerfile.temp_precip)

[Adding local temperature data to local precipitation DSS from SST model run](stormcloud/etl/temp_transfer/temperature_transfer.py)

 - Requires use of docker container as specified in [Dockerfile.temp_transfer](Dockerfile.temp_transfer)

## Identifying storms of interest

[Identify top ranked storms resulting from SST modeling runs for a specified watershed](stormcloud/etl/top_storms/extract_top_storms_dss.py)

 - Requires use of docker container as specified in [Dockerfile.top_storms_dss](Dockerfile.top_storms_dss)

## Process API support

Process API plugins exist which support both SST modeling runs and DSS file extraction from zarr data

[SST plugin directory](stormcloud/plugins/sst/)

[Temperature and precipitation directory](stormcloud/plugins/temp_precip/)

---

To test plugins, run [build_and_run.sh](build_and_run.sh) followed by either 'sst' or 'temp_precip', depending on which plugin you would like to test. Be aware that these tests result in outputs being written to s3, so test with caution