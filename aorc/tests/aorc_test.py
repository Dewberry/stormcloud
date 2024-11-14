import datetime
import json
import os
import unittest
from hashlib import sha1
from tempfile import TemporaryDirectory

from pystac import Asset, MediaType
from shapely import Polygon

from ..aorc import NULL_POLYGON, AORCItem
from .const import (
    TRINITY_TRANSPOSITION_DOMAIN_SUBSET_GEOJSON,
    TRINITY_WATERSHED_SUBSET_GEOJSON,
)


def write_geojson(geojson_data: dict, fn: str, out_dir: str) -> str:
    geojson_fn = os.path.join(out_dir, fn)
    with open(geojson_fn, "w") as f:
        json.dump(geojson_data, f)
    return geojson_fn


class AORCTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.test_directory = TemporaryDirectory()
        cls.subset_watershed_geojson_fn = write_geojson(
            TRINITY_WATERSHED_SUBSET_GEOJSON, "subset_watershed.geojson", cls.test_directory.name
        )
        cls.subset_transposition_domain_geojson_fn = write_geojson(
            TRINITY_TRANSPOSITION_DOMAIN_SUBSET_GEOJSON, "subset_transposition_domain.geojson", cls.test_directory.name
        )
        cls.test_item = AORCItem(
            "example",
            cls.subset_watershed_geojson_fn,
            "Trinity",
            cls.subset_transposition_domain_geojson_fn,
            "v01",
            datetime.datetime(2018, 9, 21),
            datetime.timedelta(hours=8),
            cls.test_directory.name,
        )

    @classmethod
    def tearDownClass(cls):
        cls.test_directory.cleanup()

    def test_aorc_paths(self):
        expected_s3_path_list = ["s3://noaa-nws-aorc-v1-1-1km/2018.zarr"]
        self.assertListEqual(expected_s3_path_list, self.test_item.aorc_paths)

    def test_aorc_source_data(self):
        expected_hash = "04975bb58fd8690cab2a7bc8fd9ea043f9a56652"
        data_dict_hash = sha1(str(self.test_item.aorc_source_data.to_dict()).encode()).hexdigest()
        self.assertEqual(expected_hash, data_dict_hash)

    def test_aorc_sum_data(self):
        expected_hash = "4913ce18f44dd16773e33e73f6b10b4b1d86bc23"
        sum_dict_hash = sha1(str(self.test_item.sum_aorc.to_dict()).encode()).hexdigest()
        self.assertEqual(expected_hash, sum_dict_hash)

    def test_valid_polygon(self):
        expected_valid_spaces_polygon = Polygon(
            [
                [-97.24714350002648, 32.61199549999999],
                [-97.24714350002648, 32.62032849999999],
                [-97.25547650002648, 32.62032849999999],
                [-97.25547650002648, 32.62866149999999],
                [-97.26380950002647, 32.62866149999999],
                [-97.26380950002647, 32.670326499999994],
                [-97.15548050002656, 32.670326499999994],
                [-97.15548050002656, 32.653660499999994],
                [-97.16381350002655, 32.653660499999994],
                [-97.16381350002655, 32.63699449999999],
                [-97.17214650002654, 32.63699449999999],
                [-97.17214650002654, 32.62866149999999],
                [-97.18047950002654, 32.62866149999999],
                [-97.18047950002654, 32.62032849999999],
                [-97.19714550002652, 32.62032849999999],
                [-97.19714550002652, 32.61199549999999],
                [-97.24714350002648, 32.61199549999999],
            ]
        )
        valid_spaces_polygon = self.test_item.valid_spaces_polygon(False, False)
        self.assertEqual(expected_valid_spaces_polygon, valid_spaces_polygon)

    def test_valid_polygon_write(self):
        expected_asset_path = os.path.join(self.test_directory.name, "valid_spaces_polygon.geojson")
        expected_asset_dict = Asset.from_dict(
            {
                "href": expected_asset_path,
                "type": MediaType.GEOJSON,
                "proj:wkt2": 'GEOGCRS["WGS 84",DATUM["World Geodetic System 1984",ELLIPSOID["WGS 84",6378137,298.257223563,LENGTHUNIT["metre",1]]],PRIMEM["Greenwich",0,ANGLEUNIT["degree",0.0174532925199433]],CS[ellipsoidal,2],AXIS["geodetic latitude (Lat)",north,ORDER[1],ANGLEUNIT["degree",0.0174532925199433]],AXIS["geodetic longitude (Lon)",east,ORDER[2],ANGLEUNIT["degree",0.0174532925199433]],ID["EPSG",4326]]',
                "proj:geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-97.24714350002648, 32.61199549999999],
                            [-97.24714350002648, 32.62032849999999],
                            [-97.25547650002648, 32.62032849999999],
                            [-97.25547650002648, 32.62866149999999],
                            [-97.26380950002647, 32.62866149999999],
                            [-97.26380950002647, 32.670326499999994],
                            [-97.15548050002656, 32.670326499999994],
                            [-97.15548050002656, 32.653660499999994],
                            [-97.16381350002655, 32.653660499999994],
                            [-97.16381350002655, 32.63699449999999],
                            [-97.17214650002654, 32.63699449999999],
                            [-97.17214650002654, 32.62866149999999],
                            [-97.18047950002654, 32.62866149999999],
                            [-97.18047950002654, 32.62032849999999],
                            [-97.19714550002652, 32.62032849999999],
                            [-97.19714550002652, 32.61199549999999],
                            [-97.24714350002648, 32.61199549999999],
                        ]
                    ],
                },
            }
        ).to_dict()
        self.test_item.valid_spaces_polygon(True, True)
        valid_polygon_asset = self.test_item.assets.get("valid_spaces_polygon")
        self.assertDictEqual(expected_asset_dict, valid_polygon_asset.to_dict())
        self.assertTrue(os.path.exists(expected_asset_path))

    def test_max_transpose(self):
        expected_transpose_polygon = Polygon(
            [
                [-97.18881250002653, 32.64532749999999],
                [-97.18881250002653, 32.670326499999994],
                [-97.15548050002656, 32.670326499999994],
                [-97.15548050002656, 32.653660499999994],
                [-97.16381350002655, 32.653660499999994],
                [-97.16381350002655, 32.64532749999999],
                [-97.18881250002653, 32.64532749999999],
            ]
        )
        expected_affine_gdal_list = [1.0, 0.0, 0.0, 1.0, 0.016665999999986525, 0.0]
        expected_stats_dict = {
            "min": {"value": 0.04724409519218084, "unit": "in"},
            "mean": {"value": 0.0508231933128006, "unit": "in"},
            "max": {"value": 0.05118110312486258, "unit": "in"},
            "count": 11,
        }
        poly, aff, stats = self.test_item.max_transpose(False)
        self.assertEqual(expected_transpose_polygon, poly)
        self.assertListEqual(expected_affine_gdal_list, list(aff.to_shapely()))
        self.assertDictEqual(expected_stats_dict, stats.to_dict())

    def test_max_transpose_properties(self):

        expected_properties_subset = {
            "aorc:statistics": {
                "min": {"value": 0.04724409519218084, "unit": "in"},
                "mean": {"value": 0.0508231933128006, "unit": "in"},
                "max": {"value": 0.05118110312486258, "unit": "in"},
                "count": 11,
            },
            "aorc:transform": [1.0, 0.0, 0.016665999999986525, 0.0, 1.0, 0.0],
        }
        self.test_item.max_transpose(True)
        self.assertTrue(
            expected_properties_subset["aorc:statistics"] == self.test_item.properties.get("aorc:statistics")
        )
        self.assertTrue(expected_properties_subset["aorc:transform"] == self.test_item.properties.get("aorc:transform"))
        self.assertNotEqual(NULL_POLYGON, self.test_item.geometry)

    def test_thumbnail_asset(self):
        self.test_item.aorc_thumbnail(0.1, True, True)
        expected_asset_path = os.path.join(self.test_directory.name, "thumbnail.png")
        self.assertTrue(os.path.exists(expected_asset_path))
        self.assertTrue(self.test_item.assets.get("thumbnail") != None)


if __name__ == "__main__":
    unittest.main()
