# stormcloud

Creates datasets for hydrologic modeling in the cloud.

---

[Workflow](workflow.md)

---

[Meilisearch](stormcloud/ms/README.md)

---

## Adding Temperature Data

[Write AORC data for precipitation, temperature, or both to DSS format](stormcloud/write_aorc_zarr_to_dss.py)

- Requires use of docker container as specified in [Dockerfile.temp_precip](Dockerfile.temp_precip)

## Identifying storms of interest

[Identify top ranked storms tracked in meilisearch database, rerun zarr to DSS conversion and save to local path](stormcloud/etl/top_storms/extract_top_storms_dss.py)

- Requires use of docker container as specified in [Dockerfile.top_storms_dss](Dockerfile.top_storms_dss)

## Getting valid transposition geometry

[Get the geometry defining all valid transposes within a transposition region for a given watershed and saves the geometry as a simple geojson polygon](stormcloud/etl/transpose_geom/get_valid_transpose_geom.py)

- Requires use of docker container as specified in [Dockerfile.transpose_geom](Dockerfile.transpose_geom)

## Plugins -- Process API support

Process API plugins exist which support both SST modeling runs and DSS file extraction from zarr data

[SST plugin directory](stormcloud/plugins/sst/)

[Temperature and precipitation extraction plugin directory](stormcloud/plugins/temp_precip/)

[Metadata standardization plugin directory](stormcloud/plugins/standardize_meta/)

[HMS GRID package generation plugin directory](stormcloud/plugins/hms_grid/)

---

To test plugins:

- edit dockerfile of plugin of interest, uncommenting the entrypoint execution block
- run [build_and_run.sh](build_and_run.sh) followed by either 'sst', 'temp_precip', 'standardize_meta', or 'hms_grid', depending on which plugin you would like to test. Be aware that these tests result in outputs being written to s3, so test with caution
