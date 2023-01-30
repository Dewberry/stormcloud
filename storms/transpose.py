from dataclasses import dataclass
from dataclasses_json import dataclass_json
import numpy as np
from shapely.geometry import Polygon
from typing import List
import xarray as xr


@dataclass_json
@dataclass
class Translate:
    x_delta: int
    y_delta: int
    indexes: np.ndarray
    data: np.ndarray
    coords: np.ndarray


class Transposer:
    def __init__(
        self,
        xsum: xr.Dataset,
        watershed_geom: Polygon,
        data_var: str = "APCP_surface",
        x_var: str = "longitude",
        y_var: str = "latitude",
    ):
        self.xsum = xsum
        self.data = xsum[data_var].to_numpy()
        self.x_coords = xsum[x_var].to_numpy()
        self.y_coords = xsum[y_var].to_numpy()

        # get watershed mask
        xmask = xsum.rio.clip([watershed_geom], drop=False, all_touched=True).copy()
        self.mask = np.isfinite(xmask[data_var].to_numpy())

        # get translates
        self.translates = self.__translates()

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
                    data_slice = self.data[transl_indexes]

                    if np.all(np.isfinite(data_slice)):

                        coords = np.column_stack(
                            (self.x_coords[transl_indexes[:, 0]], self.y_coords[transl_indexes[:, 1]])
                        )

                        translates.append(
                            Translate(
                                x_delta=x_diff, y_delta=y_diff, indexes=transl_indexes, data=data_slice, coords=coords
                            )
                        )

        return np.array(translates)

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
