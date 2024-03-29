# stormcloud

Creates datasets for hydrologic modeling in the cloud.

---

[Workflow](workflow.md)

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

Process API plugins exist which support:
- SST modeling runs
- DSS file extraction from zarr data
- Ranked document creation from SST modeling run output

## Meilisearch
Meilisearch acts as a back end for the [StormViewer site](https://storms.dewberryanalytics.com/) by holding, sorting, and filtering metadata associated with SST runs. In this repo, there are scripts which:
    - recreate the meilisearch index settings used by the project
    - query the meilisearch index for storms using user-provided filters
    - create an HMS grid package for a subsection of events from s3 data located using SST metadata
    - update the meilisearch index with ranked documents generated by the [ranked document plugin](stormcloud/plugins/doc_rank/README.md)

[SST plugin directory](stormcloud/plugins/sst/)

[Temperature and precipitation extraction plugin directory](stormcloud/plugins/temp_precip/)

[Metadata standardization plugin directory](stormcloud/plugins/standardize_meta/)

[HMS GRID package generation plugin directory](stormcloud/plugins/hms_grid/)

[Ranked document plugin directory](stormcloud/plugins/doc_rank/)

---

To test plugins:

- edit dockerfile of plugin of interest, uncommenting the entrypoint execution block
- run [build_and_run.sh](build_and_run.sh) followed by either 'sst', 'temp_precip', 'standardize_meta', 'hms_grid', or 'doc_rank' depending on which plugin you would like to test. Be aware that these tests result in outputs being written to s3, so test with caution
