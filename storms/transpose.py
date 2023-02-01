from dataclasses import dataclass
from dataclasses_json import dataclass_json
import numpy as np
from shapely.geometry import box, Polygon
from shapely.ops import unary_union
from typing import List
import xarray as xr
from scipy.stats import rankdata


@dataclass_json
@dataclass
class Translate:
    x_delta: int
    y_delta: int
    indexes: np.ndarray
    data: np.ndarray
    coords: np.ndarray
    normalized_data: np.ndarray
    _cellsize_x: float
    _cellsize_y: float

    @property
    def mean(self):
        return self.data.mean()

    @property
    def sum(self):
        return self.data.mean()

    @property
    def max(self):
        return self.data.max()

    @property
    def normalized_mean(self):
        if self.normalized_data is not None:
            return (self.data / self.normalized_data).mean()
        else:
            return None

    @property
    def geom(self):
        boxes = []
        for coord in self.coords:
            x, y = coord
            minx = x - (self._cellsize_x / 2)
            maxx = x + (self._cellsize_x / 2)
            miny = y - (self._cellsize_y / 2)
            maxy = y + (self._cellsize_y / 2)

            boxes.append(box(minx, miny, maxx, maxy))

        return unary_union(boxes)


class Transposer:
    def __init__(
        self,
        xsum: xr.Dataset,
        watershed_geom: Polygon,
        data_var: str = "APCP_surface",
        x_var: str = "longitude",
        y_var: str = "latitude",
        normalized_data: np.ndarray = None,
    ):
        self.xsum = xsum
        self.data = xsum[data_var].to_numpy()
        self.x_coords = xsum[x_var].to_numpy()
        self.y_coords = xsum[y_var].to_numpy()

        # get watershed mask
        xmask = xsum.rio.clip([watershed_geom], drop=False, all_touched=True).copy()
        self.mask = np.isfinite(xmask[data_var].to_numpy())

        # array used to normalize xsum
        self.normalized_data = normalized_data

        # get cell size for geoms
        transform = xsum.rio.transform()
        self._cellsize_x = abs(transform[0])
        self._cellsize_y = abs(transform[4])

        # get translates
        self.translates = self.__translates()

        # get valid space
        self.valid_space = self.__valid_space()

    def __translates(self) -> np.ndarray:
        translates = []
        mask_minx, mask_miny, mask_maxx, mask_maxy = self.mask_bounds
        max_x = self.xs.max()
        max_y = self.ys.max()
        mask_idxs = self.mask_idxs

        for x in self.xs:
            for y in self.ys:

                x_diff = x - mask_minx
                y_diff = y - mask_miny

                if (
                    mask_minx + x_diff >= 0
                    and mask_maxx + x_diff <= max_x
                    and mask_miny + y_diff >= 0
                    and mask_maxy + y_diff <= max_y
                ):

                    transl_indexes = np.column_stack((mask_idxs[:, 0] + x_diff, mask_idxs[:, 1] + y_diff))
                    data_slice = self.data[transl_indexes[:, 1], transl_indexes[:, 0]]

                    if np.all(np.isfinite(data_slice)):

                        coords = np.column_stack(
                            (self.x_coords[transl_indexes[:, 0]], self.y_coords[transl_indexes[:, 1]])
                        )
                        if self.normalized_data is not None:
                            norm_data = self.normalized_data[transl_indexes[:, 1], transl_indexes[:, 0]]
                        else:
                            norm_data = None
                        translates.append(
                            Translate(
                                x_delta=x_diff,
                                y_delta=y_diff,
                                indexes=transl_indexes,
                                data=data_slice,
                                coords=coords,
                                normalized_data=norm_data,
                                _cellsize_x=self._cellsize_x,
                                _cellsize_y=self._cellsize_y,
                            )
                        )

        return np.array(translates)

    def __valid_space(self):

        valid_space = np.full(self.mask.shape, False)
        for t in self.translates:
            valid_space[t.indexes[:, 1], t.indexes[:, 0]] = True

        return valid_space

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
        return np.array(range(self.shape[1]))

    @property
    def ys(self):
        """
        Y indexes
        """
        return np.array(range(self.shape[0]))

    @property
    def meshgrid(self) -> List[np.ndarray]:
        """
        Meshgrid of x,y indexes
        Returns two arrays of shape (Y, X) for the x and y indexes
        """
        return np.meshgrid(self.xs, self.ys)

    @property
    def mask_idxs(self) -> np.ndarray:
        """
        Column stack of x,y indexes.
        Indexes filtered using mask
        Returns single array of stacked x, y indexs in shape of (self.mask == True, 2)
        """
        _xs, _ys = self.meshgrid

        return np.column_stack((_xs[self.mask], _ys[self.mask]))

    @property
    def mask_bounds(self) -> tuple:
        return (
            self.mask_idxs[:, 0].min(),
            self.mask_idxs[:, 1].min(),
            self.mask_idxs[:, 0].max(),
            self.mask_idxs[:, 1].max(),
        )

    def stats(self, metric: str):
        metrics = ["mean", "max", "sum", "normalized_mean"]

        if metric in metrics:
            if metric == "mean":
                return np.array([t.mean for t in self.translates])
            elif metric == "max":
                return np.array([t.max for t in self.translates])
            elif metric == "sum":
                return np.array([t.sum for t in self.translates])
            elif metric == "normalized_mean":
                return np.array([t.normalized_mean for t in self.translates])

        else:
            raise ValueError(f"`{metric}` metric is not implemented.\nAcceptable value(s): " + ", ".join(metrics))

    def ranks(self, metric: str, rank_method: str = "ordinal", order_high_low: bool = True):
        # rank_methods = ["average", "min", "max", "dense", "ordinal"]
        if order_high_low:
            return rankdata(self.stats(metric) * -1, method=rank_method)
        else:
            return rankdata(self.stats(metric), method=rank_method)

    def valid_space_geom(self):
        cells = np.flip(np.column_stack(np.where(self.valid_space)), 1)
        coords = np.column_stack((self.x_coords[cells[:, 0]], self.y_coords[cells[:, 1]]))

        boxes = []
        for coord in coords:
            x, y = coord
            minx = x - (self._cellsize_x / 2)
            maxx = x + (self._cellsize_x / 2)
            miny = y - (self._cellsize_y / 2)
            maxy = y + (self._cellsize_y / 2)

            boxes.append(box(minx, miny, maxx, maxy))

        return unary_union(boxes)
