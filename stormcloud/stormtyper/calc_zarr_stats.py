import xarray as xr
import zarr
import numpy as np
import geopandas as gpd
import json
from typing import List, Dict, Any


def clip_ds(ds_to_clip: xr.Dataset, sst_region: gpd.GeoDataFrame) -> xr.Dataset:
    """
    Clips the dataset to the region defined by a GeoDataFrame.
    """
    ds_new_coords = ds_to_clip.assign_coords(
        lat=(("south_north", "west_east"), ds_to_clip["XLAT"].data),
        lon=(("south_north", "west_east"), ds_to_clip["XLONG"].data),
        west_east=ds_to_clip["XLONG"][0, :].data,
        south_north=ds_to_clip["XLAT"][:, 0].data,
    )

    # Set spatial dimensions
    spatial_ds = ds_new_coords.rio.set_spatial_dims(
        x_dim="west_east", y_dim="south_north", inplace=True
    )

    spatial_ds = spatial_ds.rio.write_crs("EPSG:4326", inplace=True)
    sst_region = sst_region.to_crs(spatial_ds.rio.crs)
    clipped_ds = spatial_ds.rio.clip(sst_region.geometry.values, sst_region.crs)
    return clipped_ds


def get_stats(dataset: xr.Dataset) -> Dict[str, float]:
    """
    Computes statistics for the variable in a dataset, returning a dictionary with all the stats.
    """

    var_name = list(dataset.data_vars)[0]
    data_array = dataset[var_name].astype("float32")

    stats = {
        "max": round(float(np.nanmax(data_array.values)), 4),
        "min": round(float(np.nanmin(data_array.values)), 4),
        "mean": round(float(np.nanmean(data_array.values)), 4),
        "sum": round(float(np.nansum(data_array.values)), 4),
        "std": round(float(np.nanstd(data_array.values)), 4),
        "range": round(
            float(np.nanmax(data_array.values) - np.nanmin(data_array.values)), 4
        ),
        "max_mean_diff": round(
            float(np.nanmax(data_array.values) - np.nanmean(data_array.values)), 4
        ),
        "max_mean_ratio": round(
            float(np.nanmax(data_array.values) / np.nanmean(data_array.values)), 4
        ),
    }

    return stats


def main(zarr_paths: List[str], sst_region_path: str) -> List[Dict[str, Any]]:
    """
    Processes Zarr datasets and computes daily statistics for the entire domain and a clipped region.
    """

    full_stats = []
    sst_region = gpd.read_file(sst_region_path)

    for zarr_path in zarr_paths:
        var_name = zarr_path.split("_", 1)[1].rsplit(".", 1)[0]
        zarr_store = zarr.open(zarr_path)

        for group_name in zarr_store.keys():
            ds = xr.open_zarr(zarr_path, group=group_name)
            unique_days = np.unique(ds["time"].values.astype("datetime64[D]"))[:-1]

            for day in unique_days:
                start_of_day = np.datetime64(day, "ns")
                end_of_day = start_of_day + np.timedelta64(1, "D")
                day_str = str(day)

                daily_ds = ds.sel(
                    time=slice(start_of_day, end_of_day - np.timedelta64(1, "ns"))
                )
                clipped_ds = clip_ds(daily_ds, sst_region)

                daily_domain_stats = get_stats(daily_ds)
                daily_clipped_stats = get_stats(clipped_ds)

                daily_stats_entry = {
                    day_str: {
                        var_name: {
                            "domain": daily_domain_stats,
                            "clipped": daily_clipped_stats,
                        }
                    }
                }

                existing_entry = next(
                    (entry for entry in full_stats if day_str in entry), None
                )
                if existing_entry:
                    existing_entry[day_str].update(daily_stats_entry[day_str])
                else:
                    full_stats.append(daily_stats_entry)

    return full_stats


if __name__ == "__main__":
    zarr_paths = [
        "Duwamish_SRH03.zarr",
        "Duwamish_PSFC.zarr",
        "Duwamish_IVT.zarr",
        "Duwamish_PREC_ACC_NC.zarr",
        "Duwamish_PWAT.zarr",
        "Duwamish_SBCAPE.zarr",
        "Duwamish_Z_50000Pa.zarr",
    ]
    sst_region_path = "duwamish-transpo-area-v01.geojson"
    output_file = "example_stats.json"

    full_stats = main(zarr_paths, sst_region_path)

    with open(output_file, "w") as file:
        json.dump(full_stats, file, indent=4)
