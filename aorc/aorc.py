import argparse
import datetime
import json
import os
from typing import Any, Iterator

import fiona
import numpy as np
import s3fs
import xarray as xr
from affine import Affine
from matplotlib import patches
from matplotlib import pyplot as plt
from matplotlib.figure import Figure
from pyproj import CRS, Transformer
from pystac import Asset, Collection, Item, MediaType
from pystac.extensions.projection import ProjectionExtension
from shapely import Geometry, Polygon, to_geojson
from shapely.geometry import mapping, shape
from shapely.ops import transform

from .extension.extension import (
    AccumulationMeasurementWithUnits,
    AORCExtension,
    AORCStatistics,
    Unit,
)
from .transpose import Transpose

NULL_POLYGON = Polygon()
MM_TO_INCH_CONVERSION_FACTOR = 0.03937007874015748


def read_geojson_href(href: str, **fiona_env_kwargs) -> tuple[Geometry, CRS]:
    with fiona.Env(**fiona_env_kwargs):
        with fiona.open(href, mode="r", driver="GeoJSON") as collection:
            collection: fiona.Collection
            if len(collection) != 1:
                raise ValueError(f"Collection contains {len(collection)} features; expected single feature")
            geom = shape(next(iter(collection))["geometry"])
            crs = CRS.from_user_input(collection.crs)
    return geom, crs


def convert_to_geojson_dict(geom: Geometry) -> dict[str, Any]:
    return json.loads(to_geojson(geom))


class ValidateDatetime(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        try:
            dt = datetime.datetime.strptime("%Y-%m-%d")
        except:
            try:
                dt = datetime.datetime.fromisoformat(values)
            except:
                parser.error(f"Provided date string {values} is not in %Y-%m-%d format or in isoformat")
        setattr(namespace, self.dest, dt)


class ValidateDirectory(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        if not os.path.isdir(values):
            parser.error(f"Provided value {values} is not a directory")
        setattr(namespace, self.dest, values)


class AORCItem(Item):
    NOAA_AORC_S3_BASE_URL = "s3://noaa-nws-aorc-v1-1-1km"
    AORC_X_VAR = "longitude"
    AORC_Y_VAR = "latitude"
    AORC_VARS = [
        "APCP_surface",
        "DLWRF_surface",
        "DSWRF_surface",
        "PRES_surface",
        "SPFH_2maboveground",
        "TMP_2maboveground",
        "UGRD_10maboveground",
        "VGRD_10maboveground",
    ]

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
        self._transposed_watershed: Polygon | None = None
        self._transposition_transform: Affine | None = None
        self._stats: AORCStatistics | None = None

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
        AORCExtension.add_to(self)
        ProjectionExtension.add_to(self)

    # @property
    # def is_ranked(self) -> bool:
    #     rank_property = self.properties.get("aorc:rank")
    #     if rank_property == None:
    #         return False
    #     return True

    @property
    def aorc_paths(self) -> list[str]:
        "constructs s3 paths for AORC datasets for given start time and duration"
        if self.end_datetime.year - self.start_datetime.year > 0:
            year_list = []
            current_time = self.start_datetime
            while current_time < self.end_datetime:
                year_list.append(current_time.year)
                current_time.replace(year=current_time.year + 1)
        else:
            year_list = [self.start_datetime.year]
        fileset = [f"{self.NOAA_AORC_S3_BASE_URL}/{dataset_year}.zarr" for dataset_year in year_list]
        return fileset

    @property
    def aorc_source_data(self) -> xr.Dataset:
        """
        - reads AORC data into memory as multifile dataset using s3 paths
        - doesn't read the entire ZARR files, instead just reads slice of data corresponding to transposition domain geometry and limited to start and end time
        - adds ZARR files to assets if they don't exist already
        """
        if self._aorc_source_data == None:
            s3_out = s3fs.S3FileSystem(anon=True)
            fileset = [s3fs.S3Map(root=aorc_path, s3=s3_out, check=False) for aorc_path in self.aorc_paths]
            ds = xr.open_mfdataset(fileset, engine="zarr", chunks="auto", consolidated=True)
            pyproj_crs = CRS.from_user_input(ds.rio.crs)
            if pyproj_crs != self.transposition_domain_crs:
                transform_function = Transformer.from_crs(self.transposition_domain_crs, pyproj_crs).transform
                transposition_geom_for_clip = transform(transform_function, self.transposition_domain_geometry)
            else:
                transposition_geom_for_clip = self.transposition_domain_geometry
            bounds = transposition_geom_for_clip.bounds
            subsection = ds.sel(
                time=slice(self.start_datetime, self.end_datetime),
                longitude=slice(bounds[0], bounds[2]),
                latitude=slice(bounds[1], bounds[3]),
            )
            self._aorc_source_data = subsection.rio.clip([transposition_geom_for_clip], drop=True, all_touched=True)
        return self._aorc_source_data

    @property
    def aorc_size_in_memory(self) -> int:
        "calculates amount of memory occupied by AORC data for this object"
        # if no source data is loaded, size is 0
        if self._aorc_source_data == None:
            ds_size = 0
        # if source data is loaded, size is dataset nbytes (not sure if this is true)
        else:
            ds_size = self._aorc_source_data.nbytes
        # if sum data is not calculated, size is 0
        if self._sum_aorc == None:
            da_size = 0
        # if sum data is calculated size is data array nbytes (not sure if this is true)
        else:
            da_size = self._sum_aorc.nbytes
        return ds_size + da_size

    @property
    def transpose(self) -> Transpose:
        "creates transpose class to use for transposition functions"
        if self._transpose == None:
            pyproj_crs = CRS.from_user_input(self.aorc_source_data.rio.crs)
            if pyproj_crs != self.watershed_crs:
                transform_function = Transformer.from_crs(self.watershed_crs, pyproj_crs).transform
                watershed_geom_for_transpose = transform(transform_function, self.watershed_geometry)
            else:
                watershed_geom_for_transpose = self.watershed_geometry
            self._transpose = Transpose(
                self.sum_aorc["APCP_surface"], watershed_geom_for_transpose, self.AORC_X_VAR, self.AORC_Y_VAR
            )
        return self._transpose

    @property
    def sum_aorc(self) -> xr.DataArray:
        "sums AORC precipitation data over the duration"
        if self._sum_aorc == None:
            self._sum_aorc = self.aorc_source_data.sum(dim="time", skipna=True, min_count=1)
        return self._sum_aorc

    def _write_to_geojson(self, geom: Geometry, crs: CRS, fn: str) -> str:
        outpath = os.path.join(self.local_directory, fn)
        schema = {"geometry": geom.geom_type, "properties": {}}
        with fiona.open(outpath, mode="w", driver="GeoJSON", schema=schema, crs_wkt=crs.to_wkt()) as collection:
            collection: fiona.Collection
            feature_dict = {"properties": {}, "geometry": mapping(geom)}
            collection.write(feature_dict)
        return outpath

    def valid_spaces_polygon(self, add_asset: bool = True, write: bool = True) -> Polygon:
        "converts valid spaces boolean array to a polygon"
        valid_spaces_polygon = self.transpose.valid_spaces_polygon
        if add_asset | write:
            pyproj_crs = CRS.from_user_input(self.aorc_source_data.rio.crs)
            fn = self._write_to_geojson(valid_spaces_polygon, pyproj_crs, "valid_spaces_polygon.geojson")
            asset = Asset(fn, media_type=MediaType.GEOJSON)
            proj = ProjectionExtension.ext(asset)
            proj.wkt2 = pyproj_crs.to_wkt()
            proj.geometry = convert_to_geojson_dict(valid_spaces_polygon)
            self.add_asset("valid_spaces_polygon", asset)
        return valid_spaces_polygon

    def max_transpose(self, add_properties: bool = True) -> tuple[Polygon, Affine, AORCStatistics]:
        """
        - convert max array to polygon
        - add stats object to item properties
        - record transpose centroid as item geometry
        - record max shift (as affine transform) to item properties
        - return polygon, transform, and stats
        """
        if not all([self._transposed_watershed, self._transposition_transform, self._stats]):
            self._transposed_watershed, self._transposition_transform, self._stats = self.transpose.max_transpose(
                self._create_stats
            )
        if add_properties:
            self.geometry = convert_to_geojson_dict(self._transposed_watershed.centroid)
            sst = AORCExtension.ext(self)
            sst.statistics = self._stats
            sst.transform = self._transposition_transform
        return self._transposed_watershed, self._transposition_transform, self._stats

    def aorc_thumbnail(self, scale_max: float, add_asset: bool = True, write: bool = True) -> Figure:
        """
        creates matplotlib figure showing:
        - location of transposed watershed with maximum precip accumulation
        - original location of watershed
        - valid area of transposition
        - original transposition domain
        """
        if self._transposed_watershed == None:
            self._transposed_watershed, self._transposition_transform, self._stats = self.transpose.max_transpose(
                self._create_stats
            )
        fig, ax = plt.subplots(figsize=(5, 5))
        fig.set_facecolor("w")
        colormap = plt.get_cmap("Spectral_r")
        (self.sum_aorc["APCP_surface"] * MM_TO_INCH_CONVERSION_FACTOR).plot(
            ax=ax, cmap=colormap, cbar_kwargs={"label": "Accumulation (Inches)"}, vmin=0, vmax=scale_max
        )
        valid_area_plt_polygon = patches.Polygon(
            np.column_stack(self.transpose.valid_spaces_polygon.exterior.coords.xy),
            lw=0.7,
            facecolor="none",
            edgecolor="gray",
        )
        ax.add_patch(valid_area_plt_polygon)
        transposed_watershed_plt_polygon = patches.Polygon(
            np.column_stack(self._transposed_watershed.exterior.coords.xy),
            lw=1,
            facecolor="none",
            edgecolor="black",
        )
        ax.add_patch(transposed_watershed_plt_polygon)
        ax.set(title=None, xlabel=None, ylabel=None)
        if add_asset | write:
            fn = os.path.join(self.local_directory, "thumbnail.png")
            fig.savefig(fn, bbox_inches="tight")
            asset = Asset(fn, media_type=MediaType.PNG, roles=["thumbnail"])
            self.add_asset("thumbnail", asset)
        return fig

    def dss(self, aorc_variables: list[str], add_asset: bool = False, write: bool = False) -> Any:
        """
        creates DSS file (not sure what to return as class)
        contains either precipitation or tempeerature data or both over the duration of the item for the valid transposition area
        references source data, not summed data
        """
        var_set = set(aorc_variables)
        diff = var_set.difference(self.AORC_VARS)
        if diff:
            raise ValueError(f"AORC variables provided {diff} are not in AORC variables available {self.AORC_VARS}")
        # for each aorc variable in set, create DSS file with each variable having different units, cumulative vs instantaneous setting, etc.
        # if add_asset or write is true, save to file and add DSS asset to assets
        pass

    @staticmethod
    def _create_stats(array: np.ndarray) -> AORCStatistics:
        count = np.count_nonzero(np.isfinite(array))
        stats = AORCStatistics.create(
            AccumulationMeasurementWithUnits(float(np.nanmin(array)) * MM_TO_INCH_CONVERSION_FACTOR, Unit.INCH),
            AccumulationMeasurementWithUnits(float(np.nanmean(array)) * MM_TO_INCH_CONVERSION_FACTOR, Unit.INCH),
            AccumulationMeasurementWithUnits(float(np.nanmax(array)) * MM_TO_INCH_CONVERSION_FACTOR, Unit.INCH),
            count,
        )
        return stats

    def run(self, scale_max: float) -> None:
        # load aorc data, registering sources as assets
        # calculate sum
        # calculate valid shifts
        # calculate max transpose, updating item properties using results
        # create valid area polygon, write to asset
        # create png using watershed geom, summed AORC data, and valid area polygon, write to asset
        self.max_transpose(True)
        self.valid_spaces_polygon(True, True)
        fig = self.aorc_thumbnail(scale_max, True, True)
        fig.clear()
        plt.close()
        self.check_for_null_geometry()

    def check_for_null_geometry(self) -> None:
        if self.geometry == convert_to_geojson_dict(NULL_POLYGON):
            raise ValueError(f"Geometry was never redefined in item")


# def write_to_grid(items: Iterator[AORCItem]) -> Any:
#     for item in items:
#         if not item.is_ranked:
#             raise ValueError(f"Item {item.id} is not ranked")
#     # check item for dss assets
#     # create grid entry for each dss asset associated with item
#     # create title using item rank, potentially storm type, and date
#     # create body using dss asset filename, 'dss:first_pathname' property, and 'sst:aorc_variable' property
#     # get dss pathname using property 'dss:first_pathname' from dss asset
#     # get SHG_WKT x and y using dss asset 'proj:centroid' property


def main(
    start: datetime.datetime,
    stop: datetime.datetime,
    watershed: str,
    watershed_name: str,
    transposition_domain: str,
    transposition_domain_name: str,
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
            current_time,
            duration,
            local_directory,
            href=item_path,
            **fiona_env_kwargs,
        )
        item.run()
        item.save_object()
        current_time += interval


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()

    parser.add_argument("start", type=str, action=ValidateDatetime)
    parser.add_argument("stop", type=str, action=ValidateDatetime)
    parser.add_argument("watershed", type=str, help="filepath of watershed geojson")
    parser.add_argument("watershed_name", type=str)
    parser.add_argument("transposition_domain", type=str, help="filepath of transposition domain geojson")
    parser.add_argument("transposition_domain_name", type=str)
    parser.add_argument(
        "local_directory",
        type=str,
        action=ValidateDirectory,
        help="directory where item and assets created will be saved",
    )
    parser.add_argument(
        "-d",
        "--duration_hours",
        type=int,
        required=False,
        default=72,
        help="duration of accumulation for AORC items in hours; defaults to 72 hours",
    )
    parser.add_argument(
        "-i",
        "--interval_days",
        type=int,
        required=False,
        default=1,
        help="time step separating items created; defaults to 1 day",
    )

    args = parser.parse_args()

    main(
        args.start,
        args.stop,
        args.watershed,
        args.watershed_name,
        args.transposition_domain,
        args.transposition_domain_name,
        datetime.timedelta(hours=args.duration_hours),
        args.local_directory,
        datetime.timedelta(days=args.interval_days),
    )
