# stormcloud
Creates datasets for hydrologic modeling in the cloud.

---

[Workflow](workflow.md)

---

[Batch Jobs](batch/README.md)

---

[Meilisearch](ms/README.md)

---

## Adding Temperature Data

[Transforming zarr data to DSS](extract_zarr_to_dss.py)

[Adding temperature data to precipitation products from model run](temperature_transfer.py)

## Process API support

Process API plugins exist which support both SST modeling runs and DSS file extraction from zarr data

[SST plugin directory](plugins/sst/)

[Temperature and precipitation directory](plugins/temp_precip/)

---

To test plugins, run either [build_and_run_sst.sh](build_and_run_sst.sh) or [build_and_run_temp_precip.sh](build_and_run_temp_precip.sh)