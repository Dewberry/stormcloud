import numpy as np
from shapely.geometry import Polygon
from typing import List
import xarray as xr


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
