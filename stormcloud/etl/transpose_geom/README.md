# transpose geom

## Summary

ETL which creates a geojson showing all valid "transposes", ie all windowed slides of a watershed around a transposition domain which do not touch null values

## Usage

This ETL has an associated [docker image](../../../Dockerfile.transpose_geom) which can be used to build an image useable to launch this ETL. There is also a service listed in the [docker compose file](../../../docker-compose.yml) but which is commented out. To use docker compose to build and run a container for this ETL, uncomment the service called something like "stormcloud-etl-transpose-geom"

It is necessary to have a .env file in this directory in order for the docker compose run definition to work. This .env file should follow the template of [.env.sample](./.env.sample)

### Parameters

For help on parameters used in this etl, you can use the `-h` flag after the python command (example below). This will print what the parameters are and a short description.

```shell
python get_valid_transpose_geom.py -h
```

