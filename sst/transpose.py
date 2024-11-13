from typing import Any, Callable, Iterator

import numpy as np
import rioxarray as rxr
import xarray as xr
from affine import Affine
from rasterio.features import shapes
from rasterio.mask import geometry_mask
from rasterio.windows import Window, get_data_window
from shapely import Geometry, Polygon
from shapely.affinity import translate
from shapely.geometry import shape


class Transpose:
    def __init__(self, data_array: xr.DataArray, watershed: Polygon, x_var: str, y_var: str) -> None:
        self.data_array = data_array
        self.watershed = watershed
        self.x_var = x_var
        self.y_var = y_var
        self.x_cellsize, self.y_cellsize = self.data_array.rio.resolution()
        self.transform = self.data_array.rio.transform()
        self.width = self.data_array.rio.width
        self.height = self.data_array.rio.height
        self._np_data_array = None
        self._watershed_window = None
        self._watershed_mask = None
        self._watershed_mask_clipped = None
        self._valid_shifts = None
        self._valid_spaces = None
        self._valid_spaces_polygon = None
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
        (row_start, row_stop), (col_start, col_stop) = window.toranges()
        self._watershed_mask_clipped = mask_array[row_start:row_stop, col_start:col_stop]

    @property
    def data_array_x_coords(self) -> np.ndarray:
        if not isinstance(self._data_array_x_coords, np.ndarray):
            self._data_array_x_coords = self.data_array[self.x_var].to_numpy()
        return self._data_array_x_coords

    @property
    def data_array_y_coords(self) -> np.ndarray:
        if not isinstance(self._data_array_y_coords, np.ndarray):
            self._data_array_y_coords = self.data_array[self.y_var].to_numpy()
        return self._data_array_y_coords

    @property
    def np_data_array(self) -> np.ndarray:
        if not isinstance(self._np_data_array, np.ndarray):
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
        if not isinstance(self._watershed_mask, np.ndarray):
            self._calculate_watershed_mask_and_window()
        return self._watershed_mask

    @property
    def watershed_mask_clipped(self) -> np.ndarray:
        if not isinstance(self._watershed_mask_clipped, np.ndarray):
            self._calculate_watershed_mask_and_window()
        return self._watershed_mask_clipped

    @property
    def valid_shifts(self) -> list[tuple[int, int]]:
        """
        - runs transposition using watershed, transposition domain, and summed aorc dataset
        - returns list of axis shift values applied to watershed mask to get all valid spaces
        """
        if self._valid_shifts == None:
            original_window_row_slice, original_window_col_slice = self.watershed_window.toslices()
            shifts: list[tuple[int, int]] = []
            min_x_delta = 0 - self.watershed_window.col_off
            min_y_delta = 0 - self.watershed_window.row_off
            max_x_delta = self.width - (self.watershed_window.col_off + self.watershed_window.width)
            max_y_delta = self.height - (self.watershed_window.row_off + self.watershed_window.height)
            x_delta = min_x_delta
            y_delta = min_y_delta
            while x_delta <= max_x_delta:
                while y_delta <= max_y_delta:
                    adjusted_row_start = original_window_row_slice.start + y_delta
                    adjusted_row_stop = original_window_row_slice.stop + y_delta
                    adjusted_col_start = original_window_col_slice.start + x_delta
                    adjusted_col_stop = original_window_col_slice.stop + x_delta
                    data_clipped = self.np_data_array[
                        adjusted_row_start:adjusted_row_stop, adjusted_col_start:adjusted_col_stop
                    ]
                    data_mask = np.isfinite(data_clipped)
                    combined_mask = np.logical_and(self.watershed_mask_clipped, data_mask)
                    if np.array_equal(combined_mask, self.watershed_mask_clipped):
                        shifts.append((x_delta, y_delta))
                    y_delta += 1
                x_delta += 1
                y_delta = min_y_delta
            self._valid_shifts = shifts

        return self._valid_shifts

    @property
    def valid_spaces(self) -> np.ndarray:
        """
        - initializes valid mask as just watershed mask value
        - iterates over list of shift values
        - applies shift (perhaps with np.roll) to watershed mask
        - performs logical_or with valid mask and rolled watershed mask
        - returns valid mask array
        """
        if not isinstance(self._valid_spaces, np.ndarray):
            valid_spaces = np.full(self.watershed_mask.shape, False, dtype=np.bool)
            for shift in self.valid_shifts:
                rolled = np.roll(self.watershed_mask, shift, axis=(1, 0))
                valid_spaces = np.logical_or(valid_spaces, rolled)
            self._valid_spaces = valid_spaces
        return self._valid_spaces

    def _array_to_polygon(self, arr: np.ndarray) -> Polygon:
        "convert supplied boolean array to geometry using coordinates of dataset"
        shapely_shapes = [
            shape(converted_shape) for converted_shape, _ in shapes(arr.astype(np.ubyte), arr, transform=self.transform)
        ]
        if len(shapely_shapes) != 1:
            raise ValueError(f"Expected single geometry feature, got {len(shapely_shapes)}")
        valid_spaces_geom = shapely_shapes[0]
        if valid_spaces_geom.geom_type != "Polygon":
            raise TypeError(f"Expected geometry type 'Polygon', got {valid_spaces_geom.geom_type}")
        return valid_spaces_geom

    @property
    def valid_spaces_polygon(self) -> Polygon:
        "converts valid spaces boolean array to a polygon"
        if self._valid_spaces_polygon == None:
            self._valid_spaces_polygon = self._array_to_polygon(self.valid_spaces)
        return self._valid_spaces_polygon

    def max_transpose(self, callable: Callable[[np.ndarray], Any] | None = None) -> tuple[Polygon, Affine, Any | None]:
        """
        - initializes max transpose array, max shift, and stats collection as None
        - iterates over list of shift values
        - applies shift to watershed mask
        - calculates stats
        - if stats collection has greater mean than max stats, overwrite max stats, max shift, and max transpose array
        """
        original_window_row_slice, original_window_col_slice = self.watershed_window.toslices()
        max_mean = None
        max_shift = None
        results = None
        for x_delta, y_delta in self.valid_shifts:
            adjusted_row_start = original_window_row_slice.start + y_delta
            adjusted_row_stop = original_window_row_slice.stop + y_delta
            adjusted_col_start = original_window_col_slice.start + x_delta
            adjusted_col_stop = original_window_col_slice.stop + x_delta
            data_clipped = self.np_data_array[
                adjusted_row_start:adjusted_row_stop, adjusted_col_start:adjusted_col_stop
            ]
            mean = np.nanmean(data_clipped)
            if max_mean == None or mean > max_mean:
                max_mean = mean
                max_shift = (x_delta * self.x_cellsize, y_delta * self.y_cellsize)
                if callable:
                    results = callable(data_clipped)
        poly = self._array_to_polygon(self.watershed_mask)
        poly = translate(poly, *max_shift)
        aff = Affine.translation(*max_shift)
        return poly, aff, results
