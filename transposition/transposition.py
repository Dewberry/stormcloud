import rasterio
from rasterio import features
from rasterio import Affine
from shapely.geometry import Polygon, box
import numpy as np
import geopandas as gpd
from pydsstools.heclib.dss import HecDss
from pydsstools.core import PairedDataContainer
from pydsstools.heclib.utils import gridInfo, lower_left_xy_from_transform
from IPython.display import display
import sys
import os


class Transposition:
    def __init__(self, dss_file: str, watershed_shapefile: str):
        name, extension = os.path.splitext(os.path.basename(dss_file))
        if extension != ".dss":
            raise ValueError(f"invalid extension for dss_file: {dss_file}")
        if not watershed_shapefile.endswith(".shp"):
            raise ValueError(f"invalid extension for watershed_shapefile: {dss_file}")
        self.name = name
        self.dss_file = dss_file
        self.directory = os.path.dirname(self.dss_file)
        self.transposed_dss_file = os.path.join(self.directory, f"transposed/{self.name}_transposed.dss")
        self.watershed_shapefile = watershed_shapefile
        self.watershed_gdf = gpd.read_file(self.watershed_shapefile)
        self.records = []
        if len(self.watershed_gdf) != 1:
            raise ValueError(f"{len(self.watershed_gdf)} polygons found in watershed_shapfile; expected 1")
        self.valid = False

        self.dss_profile()

    def dss_profile(self):
        with HecDss.Open(self.dss_file) as src:
            dataset = src.read_grid(src.getPathnameDict()["GRID"][0])
            self.profile = dataset.profile

    def reproject_watershed(self):
        self.watershed_projected_gdf = self.watershed_gdf.to_crs(self.dss_crs)

    def t_region(self):
        """get transposition region from limits of dss grid"""
        with HecDss.Open(self.dss_file) as src:
            if len(src.getPathnameDict()["GRID"]) < 1:
                raise IndexError(f"No grid records found in {self.dss_file}")
            dataset = src.read_grid(src.getPathnameDict()["GRID"][0])
            self.dss_crs = dataset.crs
            arr = dataset.read()
            arr[~arr.mask] = 1
            geoms = list(features.shapes(arr, transform=dataset.transform))
            geom = [Polygon([list(i) for i in geoms[0][0]["coordinates"][0]])]
            gdf = gpd.GeoDataFrame(geom, geometry=geom, crs=dataset.crs)
            gdf.drop(columns=[0], inplace=True)
            if len(gdf) < 1:
                raise ValueError(
                    f"length of t_region gdf is less than 1; created from first grid record in {self.dss_file}. "
                )
            elif len(gdf) > 1:
                raise ValueError(
                    f"length of t_region gdf is greater than 1; created from first grid record in {self.dss_file}. "
                )
            self.t_region = gdf

    def valid_transposition_region_polygon_bbox(self):
        """get valid transposition bounding box of the region polygon; i.e., the limits that the transposition region polygon can be shifted.
        minx is the difference between the min x of the non-transposed polygon and the result of (the difference between the max x of
        the non-transposed polygon and the max x of the watershed)
        """
        minx = float(self.t_region.bounds.minx - (self.t_region.bounds.maxx - self.watershed_projected_gdf.bounds.maxx))
        maxx = float(self.t_region.bounds.maxx - (self.t_region.bounds.minx - self.watershed_projected_gdf.bounds.minx))
        miny = float(self.t_region.bounds.miny - (self.t_region.bounds.maxy - self.watershed_projected_gdf.bounds.maxy))
        maxy = float(self.t_region.bounds.maxy - (self.t_region.bounds.miny - self.watershed_projected_gdf.bounds.miny))
        self.t_bbox_polygon = gpd.GeoDataFrame(
            {"geometry": [box(minx, miny, maxx, maxy)]}, geometry="geometry", crs=self.t_region.crs
        )

    def valid_transposition_region_centroid_bbox(self):
        """get valid transposition bounding box of the region centroid; i.e., the limits that the transposition region centroid can be shifted"""
        maxx = float(self.t_bbox_polygon.bounds.maxx - (self.t_region.bounds.maxx - self.t_region.centroid.x))
        minx = float(self.t_bbox_polygon.bounds.minx - (self.t_region.bounds.minx - self.t_region.centroid.x))
        maxy = float(self.t_bbox_polygon.bounds.maxy - (self.t_region.bounds.maxy - self.t_region.centroid.y))
        miny = float(self.t_bbox_polygon.bounds.miny - (self.t_region.bounds.miny - self.t_region.centroid.y))
        self.t_bbox_centroid = gpd.GeoDataFrame(
            {"geometry": [box(minx, miny, maxx, maxy)]}, geometry="geometry", crs=self.t_region.crs
        )

    def sample_xy(self):
        """randomly sample x and y using valid transposition bbox; using uniform distribution.
        may not return valid x,y.
        """
        self.transposed_x = np.random.uniform(self.t_bbox_centroid.bounds.minx, self.t_bbox_centroid.bounds.maxx)[0]
        self.transposed_y = np.random.uniform(self.t_bbox_centroid.bounds.miny, self.t_bbox_centroid.bounds.maxy)[0]

    def transpose(self):
        """transpose the transposition region gdf"""
        self.x_offset = self.transposed_x - self.t_region.centroid.x[0]
        self.y_offset = self.transposed_y - self.t_region.centroid.y[0]
        geom_translated = self.t_region.translate(self.x_offset, self.y_offset)
        self.transposed_t_region = gpd.GeoDataFrame(geom_translated, geometry=geom_translated)
        self.transposed_t_region.drop(columns=[0], inplace=True)
        if len(self.transposed_t_region) != 1:
            raise ValueError(
                f"The length of the transposed transposition region geodataframe is {len(self.transposed_t_region)}; expected a length of 1."
            )

    def check_containment(self):
        """check if the watershed is within the transposed transposition region"""
        if self.transposed_t_region.contains(self.watershed_projected_gdf)[0]:
            self.valid = True

    def grids_2_array(self):
        """read grids as array"""
        print(f"reading {self.dss_file}")
        arr_list = []
        with HecDss.Open(self.dss_file) as src:
            for record in src.getPathnameDict()["GRID"]:
                dataset = src.read_grid(record)
                arr_list.append(dataset.read().filled(fill_value=np.nan))
                self.records.append(record)
        self.arr = np.dstack(arr_list)

    def grid_stats(self):
        {
            "max grid": self.arr.max(axis=2),
            "mean grid": self.arr.mean(axis=2),
            "std grid": self.arr.std(axis=2),
            "sum grid": self.arr.sum(axis=2),
            "min grid": self.arr.min(axis=2),
            "max inc": self.arr.max(),
            "mean inc": self.arr.mean(),
            "std inc": self.arr.std(),
            "sum inc": self.arr.sum(),
            "min inc": self.arr.min(),
            "max cum": self.arr.sum(axis=2).max(),
            "mean cum": self.arr.sum(axis=2).mean(),
            "std cum": self.arr.sum(axis=2).std(),
            "sum cum": self.arr.sum(axis=2).sum(),
            "min cum": self.arr.sum(axis=2).min(),
            "max average": self.arr.mean(axis=2).max(),
            "mean average": self.arr.mean(axis=2).mean(),
            "std average": self.arr.mean(axis=2).std(),
            "sum average": self.arr.mean(axis=2).sum(),
            "min average": self.arr.mean(axis=2).min(),
        }

    def transpose_transform(self):
        """tranpose the Affine transform"""
        transform = self.profile["grid_transform"]
        cell_size = transform[0]
        x = transform[2] + self.x_offset
        y = transform[5] + self.y_offset
        self.transposed_transform = rasterio.transform.from_origin(x, y, cell_size, cell_size)

    def transpose_dss_grid(self):
        """transpose the dss grids"""
        print(f"writing {self.transposed_dss_file}")
        if not os.path.exists(os.path.dirname(self.transposed_dss_file)):
            os.makedirs(os.path.dirname(self.transposed_dss_file))
        self.profile["grid_transform"] = self.transposed_transform

        grid_info = gridInfo()
        grid_info.update([(key, value) for key, value in self.profile.items()])

        with HecDss.Open(self.transposed_dss_file) as src:
            for e, record in enumerate(self.records):
                arr = self.arr[:, :, e]
                lower_left_x, lower_left_y = lower_left_xy_from_transform(self.transposed_transform, arr.shape, 0, 0)
                grid_info.update([("opt_lower_left_x", lower_left_x), ("opt_lower_left_y", lower_left_y)])
                src.put_grid(record, arr, grid_info)

    def arr_2_tif(self, arr: np.array, tif: str, transform: Affine = None, crs: str = None):
        """export an array to tif given a transform and crs.
        default transform and crs pulled from transofrmed array.
        """
        if not crs:
            crs = self.dss_crs
        if not transform:
            transform = self.transposed_transform
        with rasterio.open(
            tif,
            "w",
            nodata=-9999,
            crs=crs,
            transform=transform,
            driver="GTiff",
            width=arr.shape[1],
            height=arr.shape[0],
            count=1,
            dtype="float32",
        ) as src:
            arr[np.isnan(arr)] = -9999
            src.write(arr, 1)


def xy_pd_dss(dss_file: str, transpositions):
    pathname = f"//x/PARAMETER VALUE///TABLE/"
    pdc = PairedDataContainer()
    pdc.pathname = pathname
    pdc.curve_no = 1
    pdc.independent_axis = list(range(1, 1 + len(transpositions)))
    pdc.data_no = len(transpositions)
    pdc.curves = np.array([[t.transposed_x for t in transpositions]])
    pdc.labels_list = [""]
    pdc.independent_type = "UNT"
    pdc.dependent_type = "UNT"
    pdc.dependent_units = "M"
    with HecDss.Open(dss_file) as src:
        src.put_pd(pdc)
    src.close()

    pathname = f"//y/PARAMETER VALUE///TABLE/"
    pdc = PairedDataContainer()
    pdc.pathname = pathname
    pdc.curve_no = 1
    pdc.independent_axis = list(range(1, 1 + len(transpositions)))
    pdc.data_no = len(transpositions)
    pdc.curves = np.array([[t.transposed_y for t in transpositions]])
    pdc.labels_list = [""]
    pdc.independent_type = "UNT"
    pdc.dependent_type = "UNT"
    pdc.dependent_units = "M"
    with HecDss.Open(dss_file) as src:
        src.put_pd(pdc)
    src.close()
    """
    Not working. pydss tries to convet all entries to float. can we modify this?


    pathname =f"//grid_name/PARAMETER VALUE///TABLE/"
    pdc = PairedDataContainer()
    pdc.pathname = pathname
    pdc.curve_no = 1
    pdc.independent_axis = list(range(1,1+len(transpositions)))
    pdc.data_no = len(transpositions)
    pdc.curves = np.array([[t.name for t in transpositions]])
    pdc.labels_list = ['']
    pdc.independent_type = 'UNT'
    pdc.dependent_type = 'UNT'
    #pdc.dependent_units = 'M'
    with HecDss.Open(dss_file) as src:
        src.put_pd(pdc)
    src.close()
    """


def main(dss_directory: str, watershed_shp: str, number_of_tranpositions: int = 1):
    """by default a transposition will be performed once for every dss file in the dss_directory.
    upping the number_of_transpositions will increase the number of transpositions to perform per dss_file
    in the dss_directory. CRS of the dss file is used.

    Args:
        dss_directory (str): directory conatining dss files with storms to transpose
        watershed_shp (str): shapefile of the watershed
        number_of_tranpositions (int, optional): number of transpositions to perform per dss file in dss_directory. Defaults to 1.
    """
    for file in os.listdir(dss_directory):
        if os.path.isfile(os.path.join(dss_directory, file)):
            file_name, extension = os.path.splitext(file)
            if extension == ".dss":
                transposition = Transposition(os.path.join(dss_directory, file), watershed_shp)
                transposition.t_region()
                transposition.reproject_watershed()
                transposition.valid_transposition_region_polygon_bbox()
                transposition.valid_transposition_region_centroid_bbox()

                transposition.t_bbox_centroid.to_file(
                    rf"C:\Users\mdeshotel\Downloads\dss\{file_name}_t_bbox_centroid.shp"
                )
                transposition.t_bbox_polygon.to_file(
                    rf"C:\Users\mdeshotel\Downloads\dss\{file_name}_t_bbox_polygon.shp"
                )
                transposition.t_region.to_file(rf"C:\Users\mdeshotel\Downloads\dss\{file_name}_t_region.shp")
                transpositions = []
                while len(transpositions) < number_of_tranpositions:
                    transposition.sample_xy()
                    transposition.transpose()
                    transposition.check_containment()
                    if transposition.valid:
                        print("valid", file_name)
                        transposition.transposed_t_region.to_file(
                            os.path.join(dss_directory, f"{file_name}_{len(transpositions)}_valid.shp")
                        )
                        transposition.grids_2_array()
                        transposition.transpose_transform()
                        transposition.transpose_dss_grid()
                        transposition.arr_2_tif(
                            transposition.arr.sum(axis=2),
                            os.path.join(dss_directory, f"{file_name}_{len(transpositions)}_valid.tif"),
                        )
                        transpositions.append(transposition)
                        transposition.valid = False

    # xy_pd_dss(r"C:\Users\mdeshotel\Downloads\dss.dss",transpositions)


if __name__ == "__main__":
    args = sys.argv
    dss_directory = args[1]
    watershed_shp = args[2]
    number_of_tranpositions = int(args[3])
    main(dss_directory, watershed_shp, number_of_tranpositions)
