from affine import Affine
from datetime import datetime, timedelta
import logging
import numpy as np
from pydsstools.heclib.dss.HecDss import Open
from pydsstools.heclib.utils import gridInfo, SHG_WKT, lower_left_xy_from_transform
from scipy.ndimage import measurements
from scipy.stats import rankdata
from shapely.geometry import Polygon
from sklearn.cluster import DBSCAN
from typing import List, Tuple
import warnings

warnings.filterwarnings("ignore")

import xarray as xr


class Clusterer:
    """
    Class used to cluster gridded values.
    The `data` parameter is a numpy array of values (e.g., precipitation).
    `target_n_cells` will be used in an algorithm to infer an appropriate threshold. This threshold
    will mask the `data` array and optimally leave `target_n_cells` to cluster on.
    `minimum_threshold` will be used to create the mask if it exceeds the "inferred threshold".

    Parameters
    ----------
    data: np.ndarray
        array of floats to cluster on
    target_n_cells: int
        number of cells the clustering algorithm will attempt to output
    minimum_threshold: float
        lowest allowed value to include in clustering
    fill_voids: bool
        set to True to fill voids in the mask
    """

    def __init__(self, data: np.ndarray, target_n_cells: int, minimum_threshold: float = 0, fill_voids: bool = True):
        self.data = data
        self.target_n_cells = target_n_cells
        self.minimum_threshold = minimum_threshold
        self.threshold, self.percentile = infer_threshold(data, target_n_cells)

        if minimum_threshold > self.threshold:
            self.mask = data >= minimum_threshold
        else:
            self.mask = data >= self.threshold

        self.__repr__ = "Clusterer"

        # fill voids in mask
        if fill_voids:
            self.mask = self.fill_mask_voids()

    @property
    def shape(self) -> tuple:
        """
        Shape of the data array
        """
        return self.data.shape

    @property
    def xs(self) -> np.ndarray:
        """
        X indexes
        """
        return np.array(range(self.data.shape[1]))

    @property
    def ys(self):
        """
        Y indexes
        """
        return np.array(range(self.data.shape[0]))

    @property
    def meshgrid(self) -> List[np.ndarray]:
        """
        Meshgrid of x,y indexes
        Returns two arrays of shape (Y, X) for the x and y indexes
        """
        return np.meshgrid(self.xs, self.ys)

    @property
    def column_stack(self) -> np.ndarray:
        """
        Column stack of x,y indexes.
        Indexes filtered using mask
        Returns single array of stacked x, y indexs in shape of (self.mask == True, 2)
        """
        _xs, _ys = self.meshgrid

        return np.column_stack((_xs[self.mask], _ys[self.mask]))

    def fill_mask_voids(self) -> np.ndarray:
        """
        Returns copy of mask with "void" cells set to True.
        A void cell is an grouping of cell(s) that do not "touch" the boundary
        """
        mask_copy = self.mask.copy()
        label, _ = measurements.label(~mask_copy)

        max_y, max_x = mask_copy.shape
        max_y = max_y - 1
        max_x = max_x - 1
        for l in np.unique(label):
            idys, idxs = np.where(label == l)
            if mask_copy[(idys, idxs)].mean() == 0:
                if not (np.any(np.isin([0, max_y], idys)) or np.any(np.isin([0, max_x], idxs))):
                    mask_copy[(idys, idxs)] = 1

        return mask_copy

    def db_cluster(self, eps: float = 1.5, min_samples: int = 1) -> np.ndarray:
        """
        Runs density-based clustering on the data array. The clustering masks the grid
        using self.mask and then clusters the x and y indexes.

        Returns an array of labels that define the clusters.
        """

        # need to handle this check
        # if len(X) > 0:

        dbscan = DBSCAN(eps=eps, min_samples=min_samples).fit(self.column_stack)

        # best way to return clusters?
        # either here or in other method
        # how to handle outliers
        return dbscan.labels_.copy()

    def get_cluster(self, labels: np.ndarray, id: int):
        """
        Returns a cluster from a label array and lable id
        """
        return Cluster(self, self.column_stack[labels == id])


class Cluster:
    """
    Class output from clustering algorithm. Each cluster is a grouping of connected grid cells
    that can be mutated to either add or remove cells inplace. Additionally, statistics can be
    calculated on a cluster.

    Parameters
    ----------
    clusterer: Clusterer
        used to access some methods and arrays from the Clusterer class
    cells: np.ndarray
        indexes of cells belonging to a cluster

    """

    def __init__(self, clusterer: Clusterer, cells: np.ndarray):
        self.cells = cells
        self.exterior = self.build_exterior_cells()
        self.interior = self.build_interior_cells()
        self.__clusterer = clusterer

    @property
    def size(self) -> int:
        """
        Returns the number of cells
        """
        return len(self.cells)

    @property
    def mean(self) -> float:
        """
        Returns the mean of cluster
        """
        return self.__clusterer.data[self.cells[:, 1], self.cells[:, 0]].mean()

    @property
    def max(self) -> float:
        """
        Returns the max of the cluster
        """
        return self.__clusterer.data[self.cells[:, 1], self.cells[:, 0]].max()

    def normalize(self) -> float:
        """
        Normalize algorithm with Atlas14 data
        """
        pass

    def disconnected(self) -> Tuple[bool, np.ndarray]:
        """
        Returns true/false if the cluster is a a single conected feature.
        Returns the labels of the separate features.
        """
        cluster_mask = np.full(self.__clusterer.mask.shape, False)
        cluster_mask[self.cells[:, 1], self.cells[:, 0]] = True
        structure = np.array([[1, 1, 1], [1, 1, 1], [1, 1, 1]])

        labels, count = measurements.label(cluster_mask, structure)

        # best way to return the disconnected clusters?
        if count > 1:
            return True, labels
        else:
            return False, labels

    def split(self, labels: np.ndarray) -> list:
        """
        Returns a list of clusters separated with the `labels` array
        """
        clusters = []
        for label in np.unique(labels):
            if label > 1:  # zero considered background
                clusters.append(Cluster(self.__clusterer, self.__clusterer.column_stack[np.where(labels == label)[0]]))

    def build_exterior_cells(self) -> np.ndarray:
        """
        Used to get all cells on the boundary.
        Returns array of the indexes on exterior of cluster
        """
        exterior_cells = []
        for cell in self.cells:
            x = cell[0]
            y = cell[1]
            xlist = [x + 1, x - 1]
            ylist = [y + 1, y - 1]
            if (
                len(self.cells[(self.cells[:, 0] == x) & np.isin(self.cells[:, 1], ylist)])
                + len(self.cells[np.isin(self.cells[:, 0], xlist) & (self.cells[:, 1] == y)])
                < 4
            ):
                exterior_cells.append([x, y])

        return np.array(exterior_cells)

    def build_interior_cells(self) -> np.ndarray:
        """
        Used to get all cells not on the boundary (interior).
        Returns an array of the indexes of the interior of cluster
        """
        interior = self.cells.copy()
        for e in self.exterior:
            interior = interior[~((interior[:, 0] == e[0]) & (interior[:, 1] == e[1]))]
        return interior

    def buffer(self) -> np.ndarray:
        """
        Returns the cells touching the exterior
        plan to add `n` parameter to buffer more than 1 cell

        Returns array of all "buffered" cells.
        Interior and exterior cells from the cluster are not included.
        """
        max_y, max_x = self.__clusterer.shape

        touching_cells = []
        transforms = [[0, 1], [1, 1], [1, 0], [1, -1], [0, -1], [-1, -1], [-1, 0], [-1, 1]]
        for e in self.exterior:
            x = e[0]
            y = e[1]

            for transf in transforms:
                tx = x + transf[0]
                ty = y + transf[1]
                if (
                    tx < max_x
                    and tx >= 0
                    and ty <= max_y
                    and ty >= 0
                    and [ty, tx] not in touching_cells
                    and not np.any((self.cells[:, 0] == tx) & (self.cells[:, 1] == ty))
                ):
                    touching_cells.append([ty, tx])

        return np.flip(np.array(sorted(touching_cells)), axis=1)

    def add_cell(self) -> np.ndarray:
        """
        Adds a cell to the cluster.
        Returns index of the added cell.
        """
        touching_cells = self.buffer()
        cell_to_add = touching_cells[np.nanargmax(self.__clusterer.data[touching_cells[:, 1], touching_cells[:, 0]])]

        # update exterior, interior, and touching cells to improve speed

        # get index to add cell
        add_idx = np.where((self.cells[:, 0] >= cell_to_add[0]) & (self.cells[:, 1] >= cell_to_add[1]))[0]

        if add_idx.size >= 1:
            self.cells = np.insert(self.cells, add_idx[0], cell_to_add, axis=0)
        else:
            add_idx = np.where(self.cells[:, 1] >= cell_to_add[1])[0]
            if add_idx.size >= 1:
                self.cells = np.insert(self.cells, add_idx[0], cell_to_add, axis=0)
            else:
                self.cells = np.append(self.cells, [cell_to_add], axis=0)

        self.exterior = self.build_exterior_cells()
        self.interior = self.build_interior_cells()

        return cell_to_add

    def remove_cell(self) -> np.ndarray:
        """
        Removes a cell from the cluster.
        Returns index of the removed cell.
        """

        # cell_to_remove = self.exterior[np.nanargmin(self.__clusterer.data[self.exterior[:, 1], self.exterior[:, 0]])]
        # updated way to get index to remove
        # add cell will grab the samllest (min) index in case of tie, remove cell will grab the greatest (max) index
        precip_data = self.__clusterer.data[self.exterior[:, 1], self.exterior[:, 0]]
        idxs = np.where(precip_data == np.nanmin(precip_data))[0]
        cell_to_remove = self.exterior[idxs.max()]

        idx = np.where((self.cells[:, 0] == cell_to_remove[0]) & (self.cells[:, 1] == cell_to_remove[1]))

        self.cells = np.delete(self.cells, idx[0][0], axis=0)

        self.exterior = self.build_exterior_cells()
        self.interior = self.build_interior_cells()

        return cell_to_remove


def infer_threshold(data: np.ndarray, target_n_cells: int) -> Tuple[float, float]:
    """
    Used to guess at the appropriate threshold to filter data array.
    """
    data = data[np.isfinite(data)]
    n_cells = data.size

    percentile = 100 - float(target_n_cells) / float(n_cells) * 100

    if percentile < 0:
        percentile = 0

    if percentile > 100:
        percentile = 100
    if np.__version__ < "1.22.0":
        threshold = np.percentile(data, percentile, interpolation="lower")
    else:
        threshold = np.percentile(data, percentile, method="lower")

    return threshold, percentile


def get_zarrfiles(data_type: str, start: datetime, end: datetime, bucket_name: str = "tempest") -> List[str]:
    """
    Fetches a sorted list of AORC zarr files for a given time range.
    The start and end of the time range is inclusive.
    Due to file naming conventions and precipatation being an hourly accumulation and temperature being
    instanteous, if provided a start time of 1990-01-01 00:00:00 and end time of 1990-01-01 01:00:00,
    the precipitation data type will return a single zarr file [1990010101.zarr] and temperature will return
    two zarr files [1990010100.zarr, 1990010101.zarr].

    Parameters
    ----------
    data_type: str
        specifies precipitation or temperature data
    start: datetime
        inclusive start time
    end: datetime
        inclusive end time
    Return
    ------
    List[str]
    """
    zarr_files = []
    data_types = ["precipitation", "temperature"]
    # check start is <= end
    if start > end:
        raise ValueError("start `{start}` > end `{end}`")

    if data_type == "precipitation":
        dt = start + timedelta(hours=1)
    elif data_type == "temperature":
        dt = start
    else:
        # duplicated from above, should add decorators for each specific file type
        raise ValueError(f"data_type `{data_type}` not found, must be one of {data_types}")

    while dt <= end:
        zarrfile = f"s3://{bucket_name}/transforms/aorc/{data_type}/{dt.year}/{dt.year}{dt.month:02}{dt.day:02}{dt.hour:02}.zarr"
        zarr_files.append(zarrfile)
        dt = dt + timedelta(hours=1)

    return zarr_files


def get_xr_dataset(
    data_type: str, start: datetime, duration: int, aggregate_method: str = None, mask: Polygon = None
) -> xr.Dataset:
    """
    Loads hourly AORC data begining at <start> for <duration> hours into an xarray dataset.
    If <aggregate_method> is provided, will aggregate data for the entire duration.
    If <aggregate method> is None, will return the data as a stacked dataset.
    If shapely polygon provided as <mask>, this method will remove all cells not touching the mask geometry.

    Parameters
    ----------
    data_type: str
        specifies precipitation or temperature data
    start: datetime
        inclusive start time
    duration: int
        number of hours to fetch from start
    aggregate_method: str
        method used to aggregate data
    mask: Polygon
        geometry used to clip output
    Return
    ------
    xr.Dataset
    """
    if duration <= 0:
        raise ValueError("duration must be greater than 0")

    if data_type == "temperature":
        end = start + timedelta(hours=duration - 1)
    else:
        end = start + timedelta(hours=duration)

    zarr_files = get_zarrfiles(data_type, start, end)

    xdata = xr.open_mfdataset(zarr_files, engine="zarr", consolidated=True)

    if mask:
        xdata = xdata.rio.clip([mask], drop=True, all_touched=True)

    if aggregate_method:
        if aggregate_method == "sum":
            xdata = xdata.sum(dim="time", skipna=True, min_count=1)

    return xdata


def number_of_cells(xdata: xr.Dataset, geom: Polygon) -> int:
    """
    Returns the number of cells that overlap with the polygon geometry

    Parameters
    ----------
    xdata: xr.Dataset
        xarray dataset to overlay on
    geom: Polygon
        shapely geometry to overlay on grid
    Return
    ------
    int
    """

    xclip = xdata.rio.clip([geom], drop=True, all_touched=True)

    data = xclip.APCP_surface.to_numpy()

    n_cells = data[np.isfinite(data)].size

    return n_cells


def write_dss(xdata, dss_path, path_a, path_b, path_c, path_f, resolution=4000):
    """
    Converts grid to SHG with cell size <resolution> and writes to DSS.
    The DSS file will be written to the path provided in <dss_path>.
    The D-part and E-part of the pathnames will be gathered from the xarray dataset.

    Parameters
    ----------
    xdata: xr.Dataset
        time-series dataset to write to DSS
    dss_path: str
        output path to write DSS file
    path_a: str
        part a of pathname - grid reference system
    path_b: str
        part b of pathname - region name
    path_c: str
        part c of pathname - data parameter
    path_f: str
        part f of pathname - data version
    resolution: int
        Resolution of cells in reproject

    """
    # xdata = xdata.fillna(0)  # UNSURE IF I NEED THIS OR NOT
    xdata = xdata.rio.reproject(SHG_WKT, resolution=resolution)
    xdata = xdata.where(xdata.APCP_surface != xdata.APCP_surface.rio.nodata)
    grid_type = "shg-time"

    cell_zero_xcoord = 0
    cell_zero_ycoord = 0

    if xdata.y.to_numpy()[-1] < xdata.y.to_numpy()[0]:
        y_coord = xdata.y.to_numpy()[-1]
    else:
        y_coord = xdata.y.to_numpy()[0]

    if xdata.x.to_numpy()[-1] < xdata.x.to_numpy()[0]:
        x_coord = xdata.x.to_numpy()[-1]
    else:
        x_coord = xdata.x.to_numpy()[0]

    affine_transform = Affine(resolution, 0.0, x_coord, 0.0, resolution, y_coord)
    wkt = xdata.rio.crs.wkt

    with Open(dss_path) as fid:
        for i, dt64 in enumerate(xdata.time.to_numpy()):
            end_dt = datetime.utcfromtimestamp((dt64 - np.datetime64("1970-01-01T00:00:00")) / np.timedelta64(1, "s"))

            data = xdata.isel(time=i).APCP_surface.to_numpy()

            data[~np.isfinite(data)] = np.nan

            if i == 0:
                lower_left_x, lower_left_y = lower_left_xy_from_transform(
                    affine_transform, data.shape, cell_zero_xcoord, cell_zero_ycoord
                )

            start_dt = end_dt - timedelta(hours=1)

            path_d = start_dt.strftime("%d%b%Y:%H%M").upper()

            if end_dt.hour == 0 and end_dt.minute == 0:
                path_e = start_dt.strftime("%d%b%Y:2400").upper()
            else:
                path_e = end_dt.strftime("%d%b%Y:%H%M").upper()

            path = f"/{path_a}/{path_b}/{path_c}/{path_d}/{path_e}/{path_f}/"

            grid_info = gridInfo()
            grid_info.update(
                [
                    ("grid_type", grid_type),
                    ("grid_crs", wkt),
                    ("grid_transform", affine_transform),
                    ("data_type", "per-cum"),
                    ("data_units", "MM"),
                    ("opt_crs_name", "WKT"),
                    ("opt_crs_type", 0),
                    ("opt_compression", "zlib deflate"),
                    ("opt_dtype", data.dtype),
                    ("opt_grid_origin", "top-left corner"),
                    ("opt_data_source", ""),
                    ("opt_tzid", ""),
                    ("opt_tzoffset", 0),
                    ("opt_is_interval", False),
                    ("opt_time_stamped", False),
                    ("opt_lower_left_x", lower_left_x),
                    ("opt_lower_left_y", lower_left_y),
                    ("opt_cell_zero_xcoord", cell_zero_xcoord),
                    ("opt_cell_zero_ycoord", cell_zero_ycoord),
                ]
            )

            fid.put_grid(path, data, grid_info)


def adjust_cluster_size(cluster: Cluster, target_n_cells: int):
    """
    Adds or removes cells from a cluster until the total number of cells matches the target number.
    If removing cells makes the cluster non-contiguous (i.e., disconnected features) the split clusters will be
    returned as a list.
    """
    while cluster.size != target_n_cells:
        if cluster.size < target_n_cells:
            cluster.add_cell()
        else:
            cluster.remove_cell()

            # check if cluster has become disconected (not contiguous)
            disconnected, labels = cluster.disconnected()

            if disconnected:
                # determine best way to add these clusters to processing pool
                split_clusters = cluster.split(labels)
                return split_clusters

    return cluster


def rank_by_mean(clusters: List[Cluster]) -> np.ndarray:
    """
    Ranks a list of clusters by their mean.
    Returns a list used to index by rank
    """
    values = []
    for cluster in clusters:
        values.append(cluster.mean)

    return rankdata(values, method="ordinal")


def rank_by_max(clusters: List[Cluster]) -> np.ndarray:
    """
    Ranks a list of clusters by their mean.
    Returns a list used to index by rank
    """
    values = []
    for cluster in clusters:
        values.append(cluster.max)

    return rankdata(values, method="ordinal")


def rank_by_norm(clusters: List[Cluster]) -> np.ndarray:
    """
    Ranks a list of clusters by their normalized mean.
    Returns a list used to index by rank
    """
    values = []
    for cluster in clusters:
        values.append(cluster.normalize())

    return rankdata(values, method="ordinal")
