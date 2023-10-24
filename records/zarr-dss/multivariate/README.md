### NOAA zarr Dataset Extraction Records

This folder contains json documentation of past NOAA zarr extraction jobs for conversion to DSS using user-provided start and end dates for AORC data variables of interest

```
{
    "watershed_name": "<Insert name of watershed>",
    "data_variables": "<AORC data variable of interest>",
    "zarr_s3_bucket": "<s3 bucket holding zarr data>",
    "geojson_s3_path": "<s3 URI of geojson determining extent to extract from NOAA dataset>",
    "start_date": "<starting date for time period to extract, in YYYY-mm-dd format>",
    "end_date": "<ending date for time period to extract, in YYYY-mm-dd format>"
}
```