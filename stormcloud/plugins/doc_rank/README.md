# doc rank

## Ranked Document Creation Plugin

### Summary

Plugin creates documents which mirror the s3 JSON output created as part of SST runs with additional attributes:

- rank
    - this includes both a "true rank" and a "declustered rank"
    - "true rank" refers to the ranking of each storm within its respective year
    - "declustered rank" refers to the ranking of each storm within its respective year with a filter applied ensuring that SST events that overlap in temporal extent are not all ranked, which would potentially count a single storm system which affects multiple runs as multiple storms; when an SST event does not pass the filter, it is assigned a declustered rank of -1

- png URL
    - this is a publicly accessible URL pointing to a PNG displaying SST model results

- tropical storms
    - this is a list of dictionaries which contain attributes of all tropical storms which overlap the SST run
    - example:
    ```JSON
    [
    {
        "id": "1979191N22264",
        "name": "BOB",
        "start": "1979-07-11",
        "end": "1979-07-12",
        "nature": "TS"
    }
    ]
    ```
- category
    - a reformulation of watershed name and transposition domain

### Input and Output Format

For explanation of required inputs, see the [plugin YAML file](./doc_rank.yaml). This section describes in greater depth the required format of input data and the anticipated format of plugin output

#### Tropical Storm Data

When providing an s3 URI pointing to tropical storm data used in populating the tropical storms attribute, the JSON in question should be in a list in the following format:

```JSON
[
    {
        "id": "unique_id1",
        "name": "NAME1",
        "start": "1979-07-11",
        "end": "1979-07-12",
        "nature": "TS"
    }
]
```

#### Ranked Document Output

Output generated by this plugin should be in a list following this format:

```JSON
[
  {
    "id": "<watershed>_<domain>_<duration>h_19790701",
    "start": {
      "datetime": "1979-07-01 00:00:00",
      "timestamp": 299635200,
      "calendar_year": 1979,
      "water_year": 1979,
      "season": "summer"
    },
    "duration": <duration>,
    "stats": {
      "count": 4295,
      "mean": 0.3757748007774353,
      "max": 1.2677165269851685,
      "min": 0,
      "sum": 1613.9527587890625,
      "norm_mean": null
    },
    "metadata": {
      "source": "AORC",
      "watershed_name": "<watershed>",
      "transposition_domain_name": "<domain>",
      "watershed_source": "s3://tempest/watersheds/trinity/trinity.geojson",
      "transposition_domain_source": "s3://tempest/watersheds/trinity/trinity-transpo-area-<domain>.geojson",
      "create_time": "2024-01-03 22:30:26.830473",
      "png": "https://tempest.s3.amazonaws.com/watersheds/<watershed>/<watershed>-transpo-area-<domain>/<duration>h/pngs/19790701"
    },
    "categories": {
      "lv10": "<watershed>",
      "lv11": "<watershed> > <domain>"
    },
    "tropical_storms": [],
    "rank": {
      "true_rank": 30,
      "declustered_rank": -1
    }
  }
]
```

##### Optional Tropical Storms Attribute
As a note, you can see that in this example content, there are no tropical storms included in the list associated with the 'tropical_storms' attribute. This is because there were no tropical storms provided in the input JSON which intersected the temporal extent of this SST run. Additonally, the 'tropical_storms' attribute is only present in cases in which an s3 URI is given with tropical storm data. Without the tropical storm s3 URI, an equivalent document but with no 'tropical_storms' attribute is created (see below)

```JSON
[
  {
    "id": "<watershed>_<domain>_<duration>h_19790701",
    "start": {
      "datetime": "1979-07-01 00:00:00",
      "timestamp": 299635200,
      "calendar_year": 1979,
      "water_year": 1979,
      "season": "summer"
    },
    "duration": <duration>,
    "stats": {
      "count": 4295,
      "mean": 0.3757748007774353,
      "max": 1.2677165269851685,
      "min": 0,
      "sum": 1613.9527587890625,
      "norm_mean": null
    },
    "metadata": {
      "source": "AORC",
      "watershed_name": "<watershed>",
      "transposition_domain_name": "<domain>",
      "watershed_source": "s3://tempest/watersheds/trinity/trinity.geojson",
      "transposition_domain_source": "s3://tempest/watersheds/trinity/trinity-transpo-area-<domain>.geojson",
      "create_time": "2024-01-03 22:30:26.830473",
      "png": "https://tempest.s3.amazonaws.com/watersheds/<watershed>/<watershed>-transpo-area-<domain>/<duration>h/pngs/19790701"
    },
    "categories": {
      "lv10": "<watershed>",
      "lv11": "<watershed> > <domain>"
    },
    "tropical_storms": [],
    "rank": {
      "true_rank": 30,
      "declustered_rank": -1
    }
  }
]
```