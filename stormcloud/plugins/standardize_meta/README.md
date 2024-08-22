# standardize meta

## Standardized Metadata Creation Plugin

### Summary

This plugin creates and saves metadata that is suitable for consumption by the [hms grid plugin](../hms_grid/README.md)

### Input

The parameters used to create the standardized metadata are taken from attributes of documents representing SST outputs tracked on meilisearch. To see more about the inputs, consult the [yaml file](./standardize_meta.yaml)

### Output

The output will be saved under the prefix associated with the associated geojson uri as a JSON document. An example output is shown below:

```json
{
  "model_extent_name": "Duwamish",
  "model_extent_geojson_s3_uri": "s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01.geojson",
  "dss_s3_uri": "s3://tempest/watersheds/duwamish/duwamish-transpo-area-v01/72h/dss/19941026.dss",
  "start_date": "1994-10-26T00:00:00",
  "end_date": "1994-10-29T00:00:00",
  "last_modification": "2023-05-11T20:00:03+00:00",
  "sample_pathnames": {
    "PRECIPITATION": "/SHG4K/DUWAMISH/PRECIPITATION/26OCT1994:0000/26OCT1994:0100/AORC/"
  },
  "shg_x": -1998315.2301178065,
  "shg_y": 2855801.5466183517,
  "overall_rank": 73,
  "rank_within_year": null,
  "overall_limit": 100,
  "top_year_limit": null,
  "data_variables": [
    "PRECIPITATION"
  ]
}
```