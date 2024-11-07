"""
Script to generate STAC items from transposition metadata
"""

# TODO: Add auth metadata to cloud datasets where applicable

import datetime
import json
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Iterator
from urllib.parse import urlparse

import boto3
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from affine import Affine
from matplotlib.figure import Figure
from mypy_boto3_s3 import S3ServiceResource
from pyproj import CRS
from pystac import Asset, Collection, Extent, Item, MediaType
from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.storage import CloudPlatform
from pystac.stac_io import DefaultStacIO
from shapely import Geometry, Point, Polygon, to_geojson
from shapely.affinity import affine_transform

from .extension.extension import (
    AccumulationMeasurementWithUnits,
    SSTExtension,
    SSTStatistics,
    Unit,
)


def convert_to_geojson_dict(geom: Geometry) -> dict[str, Any]:
    return json.loads(to_geojson(geom))


class SortOrder(Enum):
    ASC = "ascending"
    DESC = "descending"


@dataclass
class Transpose:
    x_delta: int
    y_delta: int
    x_indices: np.ndarray
    y_indices: np.ndarray
    cell_size: float
    sst_statistics: SSTStatistics
    affine: Affine = field(init=False)

    def __post_init__(self) -> None:
        self.affine = Affine.translation(self.x_delta * self.cell_size, self.y_delta * self.cell_size)


class S3StacIO(DefaultStacIO):
    def __init__(self, headers=None):
        super().__init__(headers)
        self.session = boto3.Session()
        self.s3: S3ServiceResource = self.session.resource("s3")

    def read_text(self, source: str, *_, **__) -> str:
        parsed = urlparse(url=source)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            obj = self.s3.Object(bucket, key)
            data_encoded: bytes = obj.get()["Body"].read()
            data_decoded = data_encoded.decode()
            return data_decoded
        else:
            return super().read_text(source)

    def write_text(self, dest: str, txt, *_, **__) -> None:
        parsed = urlparse(url=dest)
        if parsed.scheme == "s3":
            bucket = parsed.netloc
            key = parsed.path[1:]
            obj = self.s3.Object(bucket, key)
            obj.put(Body=txt, ContentEncoding="utf-8")
        else:
            return super().write_text(dest, txt, *_, **__)


class S3Asset(Asset):
    def __init__(
        self,
        href: str,
        region: str,
        requester_pays: bool | None = None,
        tier: str | None = None,
        title: str | None = None,
        description: str | None = None,
        media_type: str | None = None,
        roles: list[str] | None = None,
        extra_fields: dict[str, Any] | None = None,
    ):
        if not href.startswith("s3://"):
            raise ValueError(f"S3Asset href values must start with s3://")
        super().__init__(href, title, description, media_type, roles, extra_fields)
        self.ext.add(["storage"])
        self.ext.storage.apply(platform=CloudPlatform.AWS, region=region, requester_pays=requester_pays, tier=tier)


class SSTItem(Item):
    NULL_POLYGON = Polygon([0, 0], [0, 1], [1, 1], [1, 0])

    def __init__(
        self,
        id: str,
        watershed_name: str,
        watershed_geometry: Polygon,
        transposition_domain_geometry: Polygon,
        start_datetime: datetime.datetime,
        end_datetime: datetime.datetime,
        local_directory: str,
        png_scale_max: float,
        properties: dict[str, Any] = {},
        stac_extensions: list[str] | None = None,
        href: str | None = None,
        collection: Collection | None = None,
        extra_fields: dict[str, Any] | None = None,
        assets: dict[str, Asset] | None = None,
    ):
        self.watershed_name = watershed_name
        self.watershed_geometry = watershed_geometry
        self.watershed_geometry_centroid = self.watershed_geometry.centroid
        self.transposition_geometry = transposition_domain_geometry
        self.local_directory = local_directory
        self.png_scale_max = png_scale_max
        self.valid_transposition_area = None
        super().__init__(
            id,
            convert_to_geojson_dict(self.NULL_POLYGON),
            self.transposition_geometry.bounds,
            start_datetime,
            properties,
            start_datetime,
            end_datetime,
            stac_extensions,
            href,
            collection,
            extra_fields,
            assets,
        )
        self._register_sst_extension()

    def _register_sst_extension(self) -> None:
        if not SSTExtension.get_schema_uri() in self.stac_extensions:
            self.stac_extensions.append(SSTExtension.get_schema_uri())

    def fetch_aorc(self, add_asset: bool = True) -> xr.Dataset:
        # get data
        # add each AORC zarr file fetched as asset if add_asset is true
        pass

    def sum_aorc(self, data_set: xr.Dataset) -> xr.DataArray:
        pass

    def get_transposes(self, data_array: xr.DataArray) -> Iterator[Transpose]:
        mask_data_array = data_array.rio.clip([self.watershed_geometry], drop=False, all_touched=True)
        min = AccumulationMeasurementWithUnits(0, Unit.INCH)
        mean = AccumulationMeasurementWithUnits(0, Unit.INCH)
        max = AccumulationMeasurementWithUnits(0, Unit.INCH)
        count = 1
        transpose = Transpose(
            1, 1, np.array([0, 1]), np.array([0, 1]), 1.0, SSTStatistics.create(min, mean, max, count)
        )

    def create_aorc_thumbnail(
        self, data_array: xr.DataArray, transposed_gometry: Polygon, valid_area: Polygon, add_asset: bool = True
    ) -> Figure:
        # return figure
        # save to standarized filepath and add as asset if add_asset is true
        pass

    def create_valid_spaces_geom(
        self, data_array: xr.DataArray, valid_spaces: np.ndarray, add_asset: bool = True
    ) -> Polygon:
        # convert indices of valid spaces to geometry using coordinates from data array
        # save to serialized format and add as asset if add_asset is true
        pass

    def calculate_dss(self, data_set: xr.Dataset, add_asset: bool = True) -> Any:
        # create a DSS object
        # save to DSS file and add as asset if add_asset is true
        pass

    def calculate_assets_and_properties(
        self,
        add_properties: bool = True,
        add_assets: bool = True,
        return_data: bool = False,
    ) -> tuple[Figure, Polygon, Polygon, SSTStatistics]:
        data = self.fetch_aorc(add_assets)
        summed = self.sum_aorc(data)
        valid_spaces = np.full(summed.shape, False)
        best_transpose: Transpose | None = None
        for transpose in self.get_transposes(summed):
            if best_transpose == None or transpose.sst_statistics.mean > best_transpose.sst_statistics.mean:
                best_transpose = transpose
            valid_spaces[transpose.x_indices, transpose.y_indices] = True
        valid_area = self.create_valid_spaces_geom(data, valid_spaces, add_assets)
        transpose_geometry = affine_transform(self.watershed_geometry, best_transpose.affine.to_shapely())
        self.geometry = transpose_geometry
        aorc_thumbnail = self.create_aorc_thumbnail(summed, transpose_geometry, valid_area, add_assets)
        if add_properties:
            sst = SSTExtension.ext(self)
            sst.statistics = best_transpose.sst_statistics
            sst.transform = best_transpose.affine
        if return_data:
            return aorc_thumbnail, transpose_geometry, valid_area, best_transpose.sst_statistics

    def create_transposition_domain_asset(
        self,
        href: str,
        crs: CRS,
        title: str | None = None,
        description: str | None = None,
        media_type: str | MediaType | None = None,
        roles: list[str] | None = None,
        extra_fields: dict[str, Any] | None = None,
    ) -> Asset:
        transposition_domain_asset = Asset(href, title, description, media_type, roles, extra_fields)
        proj = ProjectionExtension.ext(transposition_domain_asset, add_if_missing=True)
        proj.apply(wkt2=crs.to_wkt(), geometry=convert_to_geojson_dict(self.transposition_geometry))
        return transposition_domain_asset

    def create_watershed_centroid_asset(
        self,
        href: str,
        crs: CRS,
        title: str | None = None,
        description: str | None = None,
        media_type: str | MediaType | None = None,
        roles: list[str] | None = None,
        extra_fields: dict[str, Any] | None = None,
    ) -> Asset:
        transposition_domain_asset = Asset(href, title, description, media_type, roles, extra_fields)
        proj = ProjectionExtension.ext(transposition_domain_asset, add_if_missing=True)
        proj.apply(
            wkt2=crs.to_wkt(),
            centroid={"lat": self.watershed_geometry_centroid.y, "lon": self.watershed_geometry_centroid.x},
        )
        return transposition_domain_asset
