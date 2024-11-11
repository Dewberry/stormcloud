import datetime
import json
import os
from typing import Any

import fiona
import numpy as np
import xarray as xr
from affine import Affine
from matplotlib.figure import Figure
from pyproj import CRS
from pystac import Asset, Collection, Item, MediaType
from pystac.extensions.projection import ProjectionExtension
from shapely import Geometry, Polygon, to_geojson
from shapely.affinity import affine_transform
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
    NOAA_AORC_S3_BUCKET = "noaa"

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
        super().__init__(
            id,
            NULL_POLYGON,
            self.transposition_domain_geometry.bounds,
            start_datetime,
            properties,
            start_datetime,
            start_datetime + duration,
            stac_extensions,
            href,
            collection,
            extra_fields,
            assets,
        )
        self._register_extensions()
        self._add_watershed_asset(watershed, watershed_name)
        self._add_transposition_domain_asset(transposition_domain, transposition_domain_name)
        self._transpose: Transpose | None = None
        self._watershed_mask: np.ndarray | None = None
        self._valid_spaces: np.ndarray | None = None

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
    def aorc_paths(self) -> list[str]:
        "constructs s3 paths for AORC datasets for given start time and duration"
        pass

    @property
    def aorc_source_data(self) -> xr.Dataset:
        """
        - reads AORC data into memory as multifile dataset using s3 paths
        - doesn't read the entire ZARR files, instead just reads slice of data corresponding to transposition domain geometry and limited to start and end time
        - adds ZARR files to assets if they don't exist already
        """
        pass

    @property
    def aorc_size_in_memory(self) -> int:
        "calculates amount of memory occupied by AORC data for this object"
        pass

    def transpose(self) -> Transpose:
        "creates transpose class to use for transposition functions"
        pass

    def sum_aorc(self) -> xr.DataArray:
        "sums AORC precipitation data over the duration"
        pass

    def valid_spaces_polygon(self, transpose: Transpose, add_asset: bool = True, write: bool = True) -> Polygon:
        "converts valid spaces boolean array to a polygon"
        valid_spaces_polygon = transpose.valid_spaces_polygon()
        # if add asset or write is true, save to file and add valid area asset to assets

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
    def _create_stats(masked_array: np.ma.MaskedArray) -> SSTStatistics:
        stats = SSTStatistics.create(
            masked_array.min(), masked_array.mean(), masked_array.max(), len(masked_array.compressed)
        )
        return stats

    def run(self, scale_max: float) -> None:
        # load aorc data, registering sources as assets
        # calculate sum
        # calculate valid shifts
        # calculate max transpose, updating item properties using results
        # create valid area polygon, write to asset
        # create png using watershed geom, summed AORC data, and valid area polygon, write to asset
        sum_data = self.sum_aorc()
        transpose = self.transpose()
        max_transpose_poly, max_transpose_affine, max_transpose_stats = transpose.max_transpose(self._create_stats)
        max_transpose_stats: SSTStatistics
        self.valid_spaces_polygon(transpose, True, True)
        self.aorc_thumbnail(scale_max, True, True)


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
