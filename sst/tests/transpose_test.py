import unittest

import numpy as np
import xarray as xr
from affine import Affine
from rasterio.windows import Window
from rioxarray.raster_array import RasterArray
from shapely import Polygon, to_geojson

from ..transpose import Transpose


def create_test_xr_data_array() -> xr.DataArray:
    latitudes = np.linspace(-45, 45, 5)
    longitudes = np.linspace(-90, 90, 10)

    # Create sample data (a 5x5 array of float values)
    data = np.arange(50, dtype=np.float64)
    data[0:3] = np.nan
    data = data.reshape((5, 10))

    # Create the xarray DataArray
    da = xr.DataArray(
        data=data,
        dims=["latitude", "longitude"],
        coords={"latitude": latitudes, "longitude": longitudes},
    )

    rio_da = da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    rio_da.rio.write_crs("EPSG:4326", inplace=True)

    return rio_da


def create_test_watershed() -> Polygon:
    coords = [
        [-49.73368045342443, -43.53714084896703],
        [20.69477648707425, -43.95761098348162],
        [-13.355345256885784, 24.9298388640223],
        [-49.73368045342443, -43.53714084896703],
    ]

    return Polygon(coords)


def get_mean(array: np.ma.MaskedArray) -> float:
    return array.mean()


class TransposeTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.da = create_test_xr_data_array()
        cls.watershed = create_test_watershed()
        cls.transpose = Transpose(cls.da, cls.watershed, "longitude", "latitude")

    def test_watershed_window(self):
        expected_window = Window.from_slices(rows=(0, 4), cols=(2, 7))
        self.assertEqual(expected_window, self.transpose.watershed_window)

    def test_watershed_mask(self):
        expected_array = np.array(
            [
                [False, False, True, True, True, True, True, False, False, False],
                [False, False, True, True, True, True, False, False, False, False],
                [False, False, False, True, True, True, False, False, False, False],
                [False, False, False, True, True, False, False, False, False, False],
                [False, False, False, False, False, False, False, False, False, False],
            ],
            dtype=np.bool,
        )
        self.assertTrue(np.array_equal(expected_array, self.transpose.watershed_mask))

    def test_valid_shifts(self):
        expected_valid_shifts = [(-2, 1), (-1, 1), (0, 1), (1, 0), (1, 1), (2, 0), (2, 1), (3, 0), (3, 1)]
        self.assertListEqual(expected_valid_shifts, self.transpose.valid_shifts)

    def test_valid_spaces(self):
        expected_valid_spaces = np.array(
            [
                [False, False, False, True, True, True, True, True, True, True],
                [True, True, True, True, True, True, True, True, True, True],
                [True, True, True, True, True, True, True, True, True, False],
                [False, True, True, True, True, True, True, True, True, False],
                [False, True, True, True, True, True, True, True, False, False],
            ],
            dtype=np.bool,
        )
        self.assertTrue(np.array_equal(expected_valid_spaces, self.transpose.valid_spaces))

    def test_valid_spaces_polygon(self):
        coords = [
            [-80.0, 33.75],
            [-80.0, 56.25],
            [-60.0, 56.25],
            [-40.0, 56.25],
            [-20.0, 56.25],
            [0.0, 56.25],
            [20.0, 56.25],
            [40.0, 56.25],
            [60.0, 56.25],
            [60.0, 33.75],
            [80.0, 33.75],
            [80.0, 11.25],
            [80.0, -11.25],
            [100.0, -11.25],
            [100.0, -33.75],
            [100.0, -56.25],
            [80.0, -56.25],
            [60.0, -56.25],
            [40.0, -56.25],
            [20.0, -56.25],
            [0.0, -56.25],
            [-20.0, -56.25],
            [-40.0, -56.25],
            [-40.0, -33.75],
            [-60.0, -33.75],
            [-80.0, -33.75],
            [-100.0, -33.75],
            [-100.0, -11.25],
            [-100.0, 11.25],
            [-80.0, 11.25],
            [-80.0, 33.75],
        ]

        expected_valid_spaces_polygon = Polygon(coords)
        self.assertEqual(expected_valid_spaces_polygon, self.transpose.valid_spaces_polygon())

    def test_max_transpose(self):
        max_transpose_coords = [
            [0.0, 11.25],
            [20.0, 11.25],
            [20.0, 33.75],
            [20.0, 56.25],
            [40.0, 56.25],
            [60.0, 56.25],
            [60.0, 33.75],
            [80.0, 33.75],
            [80.0, 11.25],
            [80.0, -11.25],
            [100.0, -11.25],
            [100.0, -33.75],
            [80.0, -33.75],
            [60.0, -33.75],
            [40.0, -33.75],
            [20.0, -33.75],
            [0.0, -33.75],
            [0.0, -11.25],
            [0.0, 11.25],
        ]
        expected_dict = {
            "polygon": Polygon(max_transpose_coords),
            "affine": Affine.translation(60, 22.5),
            "mean_value": 32.0,
        }
        poly, aff, mean = self.transpose.max_transpose(get_mean)
        results_dict = {"polygon": poly, "affine": aff, "mean_value": mean}
        self.assertDictEqual(expected_dict, results_dict)


if __name__ == "__main__":
    unittest.main()
