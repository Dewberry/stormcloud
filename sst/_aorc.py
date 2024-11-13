import datetime
import json
import os
from typing import Any

import fiona
import numpy as np
import s3fs
import xarray as xr
from affine import Affine
from fsspec import FSMap
from matplotlib.figure import Figure
from pyproj import CRS
from pystac import Asset, Collection, Item, MediaType
from pystac.extensions.projection import ProjectionExtension
from shapely import Geometry, Polygon, to_geojson
from shapely.geometry import shape

from .extension.extension import SSTExtension, SSTStatistics
from .transpose import Transpose

NULL_POLYGON = Polygon([0, 0], [0, 1], [1, 1], [1, 0])


def read_geojson_href(href: str, **fiona_env_kwargs) -> tuple[Geometry, CRS]:
    with fiona.Env(**fiona_env_kwargs):
        with fiona.open(href, mode="r", driver="GeoJSON") as collection:
            collection: fiona.Collection
            if len(collection) != 1:
                raise ValueError(f"Collection contains {len(collection)} features; expected single feature")
            geom = shape(next(collection)["geometry"])
            crs = CRS.from_user_input(collection.crs)
    return geom, crs


def convert_to_geojson_dict(geom: Geometry) -> dict[str, Any]:
    return json.loads(to_geojson(geom))


class AORCItem(Item):
    NOAA_AORC_S3_BASE_URL = "s3://noaa-nws-aorc-v1-1-1km"
    AORC_X_VAR = "longitude"
    AORC_Y_VAR = "latitude"

    def __init__(
        self,
        id: str,
        watershed: str,
        watershed_name: str,
        transposition_domain: str,
        transposition_domain_name: str,
        start_datetime: datetime.datetime,
        duration: datetime.timedelta,
        local_directory: str,
        properties: dict[str, Any] = {},
        stac_extensions: list[str] | None = None,
        href: str | None = None,
        collection: Collection | None = None,
        extra_fields: dict[str, Any] | None = None,
        assets: dict[str, Asset] | None = None,
        **fiona_env_kwargs,
    ):
        self.watershed_geometry, self.watershed_crs = read_geojson_href(watershed, **fiona_env_kwargs)
        self.watershed_geometry: Polygon
        self.transposition_domain_geometry, self.transposition_domain_crs = read_geojson_href(
            transposition_domain, **fiona_env_kwargs
        )
        self.transposition_domain_geometry: Polygon
        self.local_directory = local_directory
        self.start_datetime = start_datetime
        self.end_datetime = start_datetime + duration
        super().__init__(
            id,
            NULL_POLYGON,
            self.transposition_domain_geometry.bounds,
            self.start_datetime,
            properties,
            self.start_datetime,
            self.end_datetime,
            stac_extensions,
            href,
            collection,
            extra_fields,
            assets,
        )
        self._register_extensions()
        self._add_watershed_asset(watershed, watershed_name)
        self._add_transposition_domain_asset(transposition_domain, transposition_domain_name)
        self._aorc_source_data: xr.Dataset | None = None
        self._transpose: Transpose | None = None
        self._sum_aorc: xr.DataArray | None = None

    def _add_watershed_asset(self, href: str, name: str) -> Asset:
        asset = Asset(href, title=name, media_type=MediaType.GEOJSON)
        proj = ProjectionExtension.ext(asset)
        proj.wkt2 = self.watershed_crs.to_wkt()
        proj.centroid = {"lat": self.watershed_geometry.centroid.y, "lon": self.watershed_geometry.centroid.x}
        self.add_asset("watershed", asset)
        return asset

    def _add_transposition_domain_asset(self, href: str, name: str) -> Asset:
        asset = Asset(href, title=name, media_type=MediaType.GEOJSON)
        proj = ProjectionExtension.ext(asset)
        proj.wkt2 = self.transposition_domain_crs.to_wkt()
        proj.geometry = convert_to_geojson_dict(self.transposition_domain_geometry)
        self.add_asset("transposition_domain", asset)
        return asset

    def _register_extensions(self) -> None:
        SSTExtension.add_to(self)
        ProjectionExtension.add_to(self)

    @property
    def aorc_paths(self) -> list[FSMap]:
        "constructs s3 paths for AORC datasets for given start time and duration"
        s3_out = s3fs.S3FileSystem(anon=True)
        unique_years = set([self.start_datetime.year, self.end_datetime.year])
        fileset = [
            s3fs.S3Map(root=f"s3://{self.NOAA_AORC_S3_BASE_URL}/{dataset_year}.zarr", s3=s3_out, check=False)
            for dataset_year in unique_years
        ]
        return fileset

    @property
    def aorc_source_data(self) -> xr.Dataset:
        """
        - reads AORC data into memory as multifile dataset using s3 paths
        - doesn't read the entire ZARR files, instead just reads slice of data corresponding to transposition domain geometry and limited to start and end time
        - adds ZARR files to assets if they don't exist already
        """
        if self._aorc_source_data == None:
            ds = xr.open_mfdataset(self.aorc_paths, engine="zarr", chunks="auto", consolidated=True)
            bounds = self.transposition_domain_geometry.bounds
            subsection = ds.sel(
                time=slice(self.start_datetime, self.end_datetime),
                longitude=slice(bounds[0], bounds[2]),
                latitude=slice(bounds[1], bounds[3]),
            )
            self._aorc_source_data = subsection.rio.clip(
                [self.transposition_domain_geometry], drop=True, all_touched=True
            )
        return self._aorc_source_data

    @property
    def aorc_size_in_memory(self) -> int:
        "calculates amount of memory occupied by AORC data for this object"
        pass

    @property
    def transpose(self) -> Transpose:
        "creates transpose class to use for transposition functions"
        if self._transpose == None:
            self._transpose = Transpose(self.sum_aorc, self.watershed_geometry, self.AORC_X_VAR, self.AORC_Y_VAR)
        return self._transpose

    @property
    def sum_aorc(self) -> xr.DataArray:
        "sums AORC precipitation data over the duration"
        if self._sum_aorc == None:
            self._sum_aorc = self.aorc_source_data.sum(dim="time", skipna=True, min_count=1)
        return self._sum_aorc

    def valid_spaces_polygon(self, add_asset: bool = True, write: bool = True) -> Polygon:
        "converts valid spaces boolean array to a polygon"
        valid_spaces_polygon = self.transpose.valid_spaces_polygon()
        # if add asset or write is true, save to file and add valid area asset to assets

    def max_transpose(self, add_properties: bool = True) -> tuple[Polygon, Affine, SSTStatistics]:
        """
        - convert max array to polygon
        - add stats object to item properties
        - record transpose centroid as item geometry
        - record max shift (as affine transform) to item properties
        - return polygon, transform, and stats
        """
        transposed_watershed_polygon, transposition_transform, sst_stats = self.transpose.max_transpose(
            self._create_stats
        )
        # if add_properties is true, update the internal item geometry to be the centroid of the transposed geometry, add the sst statistics and transform to item properties using the SST extension

    def aorc_thumbnail(self, scale_max: float, add_asset: bool = True, write: bool = True) -> Figure:
        """
        creates matplotlib figure showing:
        - location of transposed watershed with maximum precip accumulation
        - original location of watershed
        - valid area of transposition
        - original transposition domain
        """
        # if add_asset or write is true, save to file and add thumbnail asset to assets
        pass

    def dss(self, add_asset: bool = False, write: bool = False) -> Any:
        """
        creates DSS file (not sure what to return as class)
        contains either precipitation or tempeerature data or both over the duration of the item for the valid transposition area
        references source data, not summed data
        """
        # if add_asset or write is true, save to file and add DSS asset to assets
        pass

    @staticmethod
    def _create_stats(array: np.ndarray) -> SSTStatistics:
        count = np.count_nonzero(np.isfinite(array))
        stats = SSTStatistics.create(array.min(), array.mean(), array.max(), count)
        return stats

    def run(self, scale_max: float) -> None:
        # load aorc data, registering sources as assets
        # calculate sum
        # calculate valid shifts
        # calculate max transpose, updating item properties using results
        # create valid area polygon, write to asset
        # create png using watershed geom, summed AORC data, and valid area polygon, write to asset
        max_transpose_poly, max_transpose_affine, max_transpose_stats = self.transpose.max_transpose(self._create_stats)
        max_transpose_stats: SSTStatistics
        self.valid_spaces_polygon(True, True)
        self.aorc_thumbnail(scale_max, True, True)

    def check_for_null_geometry(self) -> None:
        if self.geometry == convert_to_geojson_dict(NULL_POLYGON):
            raise ValueError(f"Geometry was never redefined in item")


def main(
    start: datetime.datetime,
    stop: datetime.datetime,
    watershed: str,
    watershed_name: str,
    transposition_domain: str,
    transposition_domain_name: str,
    start_datetime: datetime.datetime,
    duration: datetime.timedelta,
    local_directory: str,
    interval: datetime.timedelta = datetime.timedelta(days=1),
    **fiona_env_kwargs,
):
    current_time = start
    index = 0

    while current_time < stop:
        id = f"example_{index}"
        item_path = os.path.join(local_directory, f"{id}.json")
        item = AORCItem(
            id,
            watershed,
            watershed_name,
            transposition_domain,
            transposition_domain_name,
            start_datetime,
            duration,
            local_directory,
            href=item_path,
            **fiona_env_kwargs,
        )
        item.run()
        item.save_object()
        current_time += interval
