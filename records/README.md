### Records

This folder contains json documentation of past jobs submitted in the following format

```
{
    "watershed_name": "<Insert name of watershed>",
    "domain_name": "<Insert domain name or version of transposition region. Should be in format similar to V01, indicating version",
    "duration": <int duration>,
    "atlas_14_uri": "<s3 path of atlas14 raster data used for normalization",
    "job_def": "<aws job definition to use when submitting batch jobs>",
    "job_queue": "<aws job queue to use>",
}
```

Optionally, the json can be extended with the following attributes:
```
{
    "s3_bucket": "<Bucket in which watershed and domain geojson files are stored. Also the bucket used to store model output. Defaults to 'tempest'>",
    "por_start": "<Datetime of start of period of interest. Should be in %Y-%m-%d %H:%M format. Defaults to '1979-02-01 00:00'>",
    "por_end": "<Datetime of end of period of interest. Should be in %Y-%m-%d %H:%M format. Defaults to '2022-12-31 23:00'>"
    "scale": <float inches to use as max precip in scale used for PNG image generation. Defaults to 12.0>
}
```

These json records are used as inputs both for the submission of jobs and, once jobs have been successfully completed, for the meilisearch update function.