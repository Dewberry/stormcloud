### NOAA zarr Dataset Extraction Records

This folder contains json documentation of past NOAA zarr extraction jobs submitted in the following format

```
{
    "watershed_name": "<Insert name of watershed>",
    "domain_name": "<Insert domain name or version of transposition region. Should be in format similar to V01, indicating version",
    "year": <Year of interest>,
    "n_storms": <Number of years to select from year, ranked by mean precipitation in 72 hour period>,
    "declustered": <Boolean, true if ranking of storms should 'decluster' storms, ensuring that top storms do not have overlapping time periods>,
    "data_variables": ["<Data variable 1 available from NOAA data>", ... "<Data variable n available from NOAA data>"],
    "zarr_s3_path": "<s3 URI of NOAA zarr dataset>",
    "geojson_s3_path": "<s3 URI of geojson determining extent to extract from NOAA dataset>"
}
```