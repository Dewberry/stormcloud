import unittest
from math import floor

import numpy as np
import xarray as xr
from affine import Affine
from rasterio.windows import Window
from shapely import Polygon

from ..transpose import Transpose
from .const import TRINITY_TRANSPOSITION_DOMAIN_GEOJSON, TRINITY_WATERSHED_GEOJSON


def create_test_xr_data_array() -> xr.DataArray:
    coords = TRINITY_TRANSPOSITION_DOMAIN_GEOJSON["features"][0]["geometry"]["coordinates"][0][0]
    transposition_geometry = Polygon(coords)
    min_x, min_y, max_x, max_y = transposition_geometry.bounds
    nx = floor((max_x - min_x) / 1.5)
    ny = floor((max_y - min_y) / 1.5)
    longitudes, latitudes = np.linspace(min_x, max_x, nx), np.linspace(min_y, max_y, ny)

    # Create sample data
    data = np.arange(nx * ny, dtype=np.float64)
    data = data.reshape((ny, nx))

    # Create the xarray DataArray
    da = xr.DataArray(
        data=data,
        dims=["latitude", "longitude"],
        coords={"latitude": latitudes, "longitude": longitudes},
    )

    rio_da = da.rio.set_spatial_dims(x_dim="longitude", y_dim="latitude")
    rio_da.rio.write_crs("EPSG:4326", inplace=True)
    rio_da = rio_da.rio.clip([transposition_geometry], all_touched=True, drop=False)

    return rio_da


def create_test_watershed() -> Polygon:
    coords = TRINITY_WATERSHED_GEOJSON["features"][0]["geometry"]["coordinates"][0][0]
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
        expected_window = Window.from_slices(rows=(2, 5), cols=(0, 4))
        self.assertEqual(expected_window, self.transpose.watershed_window)

    def test_watershed_mask(self):
        expected_array = np.array(
            [
                [False, False, False, False, False, False, False, False, False, False, False],
                [False, False, False, False, False, False, False, False, False, False, False],
                [False, False, True, True, False, False, False, False, False, False, False],
                [False, True, True, False, False, False, False, False, False, False, False],
                [True, True, True, False, False, False, False, False, False, False, False],
                [False, False, False, False, False, False, False, False, False, False, False],
            ],
            dtype=bool,
        )
        self.assertTrue(np.array_equal(expected_array, self.transpose.watershed_mask))

    def test_valid_shifts(self):
        expected_valid_shifts = [
            (0, -1),
            (0, 0),
            (1, 0),
            (2, 0),
            (3, 0),
            (4, 0),
            (4, 1),
            (5, 0),
            (5, 1),
            (6, 0),
            (7, 0),
        ]
        self.assertListEqual(expected_valid_shifts, self.transpose.valid_shifts)

    def test_valid_spaces(self):
        expected_valid_spaces = np.array(
            [
                [False, False, False, False, False, False, False, False, False, False, False],
                [False, False, True, True, False, False, False, False, False, False, False],
                [False, True, True, True, True, True, True, True, True, True, True],
                [True, True, True, True, True, True, True, True, True, True, False],
                [True, True, True, True, True, True, True, True, True, True, False],
                [False, False, False, False, True, True, True, True, False, False, False],
            ],
            dtype=bool,
        )
        self.assertTrue(np.array_equal(expected_valid_spaces, self.transpose.valid_spaces))

    def test_valid_spaces_polygon(self):
        coords = [
            [-96.69091696873322, 27.68900724124264],
            [-96.69091696873322, 29.496139685072745],
            [-98.44300750845167, 29.496139685072745],
            [-98.44300750845167, 31.303272128902847],
            [-100.19509804817012, 31.303272128902847],
            [-100.19509804817012, 34.91753701656306],
            [-93.18673588929632, 34.91753701656306],
            [-93.18673588929632, 36.72466946039316],
            [-86.17837373042252, 36.72466946039316],
            [-86.17837373042252, 34.91753701656306],
            [-82.67419265098562, 34.91753701656306],
            [-82.67419265098562, 31.303272128902847],
            [-80.92210211126717, 31.303272128902847],
            [-80.92210211126717, 29.496139685072745],
            [-93.18673588929632, 29.496139685072745],
            [-93.18673588929632, 27.68900724124264],
            [-96.69091696873322, 27.68900724124264],
        ]
        expected_valid_spaces_polygon = Polygon(coords)
        self.assertEqual(expected_valid_spaces_polygon, self.transpose.valid_spaces_polygon)

    def test_max_transpose(self):
        max_transpose_coords = [
            [-89.68255480985941, 31.303272128902847],
            [-89.68255480985941, 33.11040457273295],
            [-91.43464534957786, 33.11040457273295],
            [-91.43464534957786, 34.91753701656306],
            [-93.18673588929632, 34.91753701656306],
            [-93.18673588929632, 36.724669460393166],
            [-87.93046427014096, 36.724669460393166],
            [-87.93046427014096, 33.11040457273295],
            [-86.1783737304225, 33.11040457273295],
            [-86.1783737304225, 31.303272128902847],
            [-89.68255480985941, 31.303272128902847],
        ]
        expected_dict = {
            "polygon": Polygon(max_transpose_coords),
            "affine": Affine.translation(7.008362158873803, 1.807132443830104),
            "mean_value": 49.5,
        }
        poly, aff, mean = self.transpose.max_transpose(get_mean)
        results_dict = {"polygon": poly, "affine": aff, "mean_value": mean}
        self.assertDictEqual(expected_dict, results_dict)


if __name__ == "__main__":
    unittest.main()
