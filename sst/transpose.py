from typing import Any, Callable

import numpy as np
import xarray as xr
from affine import Affine
from rasterio.mask import geometry_mask
from rasterio.windows import Window, get_data_window
from shapely import Polygon, box, unary_union


class Transpose:
    def __init__(self, data_array: xr.DataArray, watershed: Polygon, x_var: str, y_var: str) -> None:
        self.data_array = data_array
        self.watershed = watershed
        self.x_var = x_var
        self.y_var = y_var
        self.x_cellsize, self.y_cellsize = self.data_array.rio.resolution
        self.width = self.data_array.rio.width
        self.height = self.data_array.rio.height
        self._np_data_array = None
        self._watershed_window = None
        self._watershed_mask = None
        self._watershed_count = None
        self._valid_spaces = None
        self._data_array_x_coords = None
        self._data_array_y_coords = None

    def _calculate_watershed_mask_and_window(self) -> None:
        mask_array = geometry_mask(
            [self.watershed],
            out_shape=(int(self.data_array.rio.height), int(self.data_array.rio.width)),
            transform=self.data_array.rio.transform(recalc=True),
            all_touched=True,
            invert=True,
        )
        self._watershed_mask = mask_array
        window = get_data_window(np.ma.masked_array(mask_array, ~mask_array))
        self._watershed_window = window

    @property
    def data_array_x_coords(self) -> np.ndarray:
        if self._data_array_x_coords == None:
            self._data_array_x_coords = self.data_array[self.x_var].to_numpy()
        return self._data_array_x_coords

    @property
    def data_array_y_coords(self) -> np.ndarray:
        if self._data_array_y_coords == None:
            self._data_array_y_coords = self.data_array[self.y_var].to_numpy()
        return self._data_array_y_coords

    @property
    def np_data_array(self) -> np.ndarray:
        if self._np_data_array == None:
            self._np_data_array = self.data_array.to_numpy()
        return self._np_data_array

    @property
    def watershed_window(self) -> Window:
        "creates Window object indicating position of watershed in domain"
        if self._watershed_window == None:
            self._calculate_watershed_mask_and_window()
        return self._watershed_window

    @property
    def watershed_mask(self) -> np.ndarray:
        "creates 2D boolean numpy array with true values where the watershed lies in the dataset"
        if self._watershed_mask == None:
            self._calculate_watershed_mask_and_window()
        return self._watershed_mask

    @property
    def valid_shifts(self) -> list[tuple[int, int]]:
        """
        - runs transposition using watershed, transposition domain, and summed aorc dataset
        - returns list of axis shift values applied to watershed mask to get all valid spaces
        """
        shifts: list[tuple[int, int]] = []
        if self._valid_spaces == None:
            min_x_delta = 0 - self.watershed_window.col_off
            min_y_delta = 0 - self.watershed_window.row_off
            max_x_delta = self.width - self.watershed_window.width
            max_y_delta = self.height - self.watershed_window.height
            x_delta = min_x_delta
            y_delta = min_y_delta
            while x_delta <= max_x_delta:
                while y_delta <= max_y_delta:
                    shift = (x_delta, y_delta)
                    rolled_mask = np.roll(self.watershed_mask, shift=(x_delta, y_delta), axis=(0, 1))
                    bool_slice = np.isfinite(self.np_data_array[[rolled_mask]])
                    bool_slice = np.logical_and(bool_slice, rolled_mask)
                    if np.array_equal(bool_slice, rolled_mask):
                        shifts.append(shift)
                    y_delta += 1
                x_delta += 1
        self._valid_spaces = shifts

        return self._valid_spaces

    @property
    def valid_spaces(self) -> np.ndarray:
        """
        - initializes valid mask as just watershed mask value
        - iterates over list of shift values
        - applies shift (perhaps with np.roll) to watershed mask
        - performs logical_or with valid mask and rolled watershed mask
        - returns valid mask array
        """
        valid_spaces = self.watershed_mask.copy()
        for shift in self.valid_shifts:
            rolled = np.roll(valid_spaces, shift, axis=(0, 1))
            valid_spaces = np.logical_or(valid_spaces, rolled)
        return valid_spaces

    def _array_to_polygon(self, arr: np.ndarray) -> Polygon:
        "convert supplied boolean array to geometry using coordinates of dataset"
        cells = np.flip(np.column_stack(np.where(self.valid_spaces)), 1)
        coords = np.column_stack((self.data_array_x_coords[cells[:, 0]], self.data_array_y_coords[cells[:, 1]]))

        boxes = []
        for coord in coords:
            x, y = coord
            minx = x - (self.x_cellsize / 2)
            maxx = x + (self.x_cellsize / 2)
            miny = y - (self.y_cellsize / 2)
            maxy = y + (self.y_cellsize / 2)

            boxes.append(box(minx, miny, maxx, maxy))

        return unary_union(boxes)

    def valid_spaces_polygon(self) -> Polygon:
        "converts valid spaces boolean array to a polygon"
        valid_spaces_polygon = self._array_to_polygon(self.valid_spaces)
        return valid_spaces_polygon

    def max_transpose(self, callable: Callable[[np.ma.MaskedArray], Any]) -> tuple[Polygon, Affine, Any]:
        """
        - initializes max transpose array, max shift, and stats collection as None
        - iterates over list of shift values
        - applies shift to watershed mask
        - calculates stats
        - if stats collection has greater mean than max stats, overwrite max stats, max shift, and max transpose array
        - convert max array to polygon
        - add stats object to item properties
        - record transpose centroid as item geometry
        - record max shift (as affine transform) to item properties
        - return polygon and stats
        """
        mask = self.watershed_mask.copy()
        max_mean = None
        max_masked_arr = None
        max_shift = None
        results = None
        for shift in self.valid_shifts:
            rolled = np.roll(mask, shift, axis=(0, 1))
            nodata_mask = ~np.isfinite(self.np_data_array)
            combined_mask = np.logical_or(~rolled, nodata_mask)
            masked_array = np.ma.masked_array(self.np_data_array, mask=combined_mask)
            mean = masked_array.mean()
            if max_mean == None or mean > max_mean:
                max_mean = mean
                max_masked_arr = masked_array
                max_shift = shift
                results = callable(masked_array)
        poly = self._array_to_polygon(max_masked_arr)
        aff = Affine.translation(*max_shift)
        return poly, aff, results
