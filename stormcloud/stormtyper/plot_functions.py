import datetime
import logging

import boto3
import cartopy
import cartopy.crs as ccrs
import geopandas as gpd
import matplotlib.colors as colors
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import xarray as xr
from dotenv import find_dotenv, load_dotenv
from matplotlib.colors import LinearSegmentedColormap
from shapely.geometry.base import BaseGeometry

from constants import M_TO_IN_FACTOR, MM_TO_IN_FACTOR, WATERSHED_FILE_LOCATION

load_dotenv(find_dotenv())
session = boto3.session.Session()
s3_client = session.client("s3")


def fetch_watershed_geom(
    watershed_file: str,
    combine_all_geoms: bool = False,
) -> BaseGeometry:
    """Fetches geometry object from watershed geojson"""

    logging.debug("Getting watershed geometry")
    watershed_geom = gpd.read_file(watershed_file)
    # Check that the geodataframe isn't empty
    if watershed_geom.empty:
        logging.error("Geodataframe is empty")
        raise ValueError("Geodataframe is empty")
    watershed_geom = watershed_geom.explode(index_parts=True)

    # Check that the geodataframe contains valid geometries
    if not watershed_geom.geometry.is_valid.all():
        logging.error("Invalid geometries present.")
        raise ValueError("Invalid geometries present in the geodataframe.")

    if combine_all_geoms:
        geom = watershed_geom.geometry.unary_union
    else:
        geom = watershed_geom.loc[0].geometry

    return geom


watershed_poly = fetch_watershed_geom(WATERSHED_FILE_LOCATION)


def setup_plot(ax, lon, lat, lon_step=50, lat_step=50):
    """General setup for plots"""
    ax.add_feature(cartopy.feature.BORDERS, linestyle="-", alpha=0.5)
    ax.add_feature(cartopy.feature.STATES, linestyle="-", alpha=0.2)
    ax.coastlines(resolution="50m")

    lon_labels = lon[0, ::lon_step].values
    lat_labels = lat[::lat_step, 0].values

    ax.set_xticks(lon[0, ::lon_step].values, crs=cartopy.crs.PlateCarree())
    ax.set_xticklabels([round(label, 2) for label in lon_labels])
    ax.set_yticks(lat[::lat_step, 0].values, crs=cartopy.crs.PlateCarree())
    ax.set_yticklabels([round(label, 2) for label in lat_labels])

    ax.set_ylabel("Latitude (degrees north)")
    ax.set_xlabel("Longitude (degrees east)")


def plot_SLP(ds: xr.Dataset, var: str):
    """Plots sea level pressure"""
    plt.figure(figsize=(12, 6))

    var_array = ds[var][0, :, :]
    lon = var_array["XLONG"]
    lat = var_array["XLAT"]
    date_str = ds["Time"].values[0].astype(str)
    formatted_date = pd.to_datetime(date_str).strftime("%Y-%m-%d_%Hz")
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_plot(ax, lon, lat)
    max_cont = 1060
    min_cont = 960
    contour_interval = 4
    levels = np.arange(min_cont, max_cont, contour_interval)
    contours = plt.contour(lon, lat, var_array, levels=levels, colors="Black")
    cf = plt.contourf(lon, lat, var_array, levels=levels, cmap="Blues_r")
    for geom in watershed_poly:
        ax.add_geometries(
            [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
        )
    plt.clabel(contours, inline=True, fontsize=10)

    plt.colorbar(cf, label=f"SLP (hPa)")
    plt.title(f"{var} @ {formatted_date}")


def plot_SBCAPE(ds: xr.Dataset, var: str):
    """Plots surface based CAPE"""
    plt.figure(figsize=(12, 6))
    var_array = ds[var][0, :, :]
    lon = var_array["XLONG"]
    lat = var_array["XLAT"]
    data_values = var_array.values
    date_str = ds["Time"].values[0].astype(str)
    formatted_date = pd.to_datetime(date_str).strftime("%Y-%m-%d_%Hz")
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_plot(ax, lon, lat)
    levels = [50, 250, 500, 1000, 2000, 3000, 4000, 6000]
    cf = plt.contourf(
        lon, lat, data_values, levels=levels, cmap=plt.cm.turbo, vmin=250, vmax=4000
    )
    for geom in watershed_poly:
        ax.add_geometries(
            [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
        )

    plt.colorbar(cf, label="SBCAPE J kg^-1")
    plt.title(f"{var} @ {formatted_date}")


def plot_PWAT(ds: xr.Dataset, var: str):
    """Plots precipitable water"""
    plt.figure(figsize=(12, 6))

    var_array = ds[var][0, :, :]
    lon = var_array["XLONG"]
    lat = var_array["XLAT"]
    # convert meters to inches
    data_values = var_array.values * M_TO_IN_FACTOR
    date_str = ds["Time"].values[0].astype(str)
    formatted_date = pd.to_datetime(date_str).strftime("%Y-%m-%d_%Hz")
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_plot(ax, lon, lat)
    levels = [0, 0.25, 0.50, 0.75, 1, 1.25, 1.50, 1.75, 2, 2.25, 2.50, 2.75]
    cf = plt.contourf(
        lon,
        lat,
        data_values,
        levels=levels,
        cmap=plt.cm.terrain_r,
        vmin=-1,
        extend="max",
    )
    for geom in watershed_poly:
        ax.add_geometries(
            [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
        )
    plt.colorbar(cf, label="PWAT (in)")
    plt.title(f"{var} @ {formatted_date}")


def plot_PREC_ACC_NC(
    ds_roll: xr.Dataset,
    ds_accum: xr.Dataset,
    var: str,
    end_time: datetime.datetime,
    precip_accum_interval: int,
):
    "Plots accumulated precipitation"
    fig, axs = plt.subplots(
        1, 2, figsize=(24, 6), subplot_kw={"projection": ccrs.PlateCarree()}
    )
    datasets = [ds_roll, ds_accum]
    titles = [
        f"{precip_accum_interval}hr Accumulated Precip",
        "Total Accumulated Precip",
    ]
    for ax, ds, title in zip(axs, datasets, titles):
        var_array = ds[var][:, :]
        lon = var_array["XLONG"]
        lat = var_array["XLAT"]
        data_values = (
            var_array.values / MM_TO_IN_FACTOR
        )  # Assuming MM_TO_IN_FACTOR is defined elsewhere
        formatted_end_time = end_time.strftime("%Y-%m-%d_%Hz")

        setup_plot(ax, lon, lat)
        ax.set_title(f"{title} @ {formatted_end_time}")

        colors = [
            (0.0, "blue"),
            (0.05, "green"),
            (0.15, "yellow"),
            (0.40, "orange"),
            (0.7, "red"),
            (1, "purple"),
        ]
        custom_cmap = LinearSegmentedColormap.from_list("custom_colormap", colors)
        levels = [0.001, 0.1, 0.25, 0.5, 1, 1.5, 2, 3, 4, 5, 6, 8, 10, 15, 20]
        cf = ax.contourf(
            lon, lat, data_values, levels=levels, cmap=custom_cmap, extend="max"
        )

        for geom in watershed_poly:
            ax.add_geometries(
                [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
            )

    fig.colorbar(
        cf,
        ax=axs.ravel().tolist(),
        label=f"Accumulated Precip (in)",
    )


def plot_SRH03(ds: xr.Dataset, var: str):
    """Plots 0-3km storm relative helicity(SRH)"""
    plt.figure(figsize=(12, 6))

    var_array = ds[var][0, :, :]
    lon = var_array["XLONG"]
    lat = var_array["XLAT"]
    data_values = var_array.values
    date_str = ds["Time"].values[0].astype(str)
    formatted_date = pd.to_datetime(date_str).strftime("%Y-%m-%d_%Hz")
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_plot(ax, lon, lat)
    colors = [
        (0.0, "grey"),
        (0.07, "green"),
        (0.15, "yellow"),
        (0.19, "orange"),
        (0.4, "red"),
        (1, "purple"),
    ]

    custom_cmap = LinearSegmentedColormap.from_list("custom_colormap", colors)

    levels = [1, 50, 75, 100, 150, 200, 300, 400, 500, 700, 900, 1100, 1500]
    cf = plt.contourf(lon, lat, data_values, levels=levels, cmap=custom_cmap)
    for geom in watershed_poly:
        ax.add_geometries(
            [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
        )
    plt.colorbar(cf, label=f"Storm Relative Helecity m2 s-2")
    plt.title(f"{var} @ {formatted_date}")


def plot_Z_50000Pa(ds: xr.Dataset, var: str):
    """Plots 500mb geopotential height"""
    plt.figure(figsize=(12, 6))

    var_array = ds[var][0, :, :]
    lon = var_array["XLONG"]
    lat = var_array["XLAT"]
    data_values = var_array.values
    date_str = ds["Time"].values[0].astype(str)
    formatted_date = pd.to_datetime(date_str).strftime("%Y-%m-%d_%Hz")
    ax = plt.axes(projection=ccrs.PlateCarree())
    setup_plot(ax, lon, lat, lon_step=75, lat_step=75)
    max_cont = 6060
    min_cont = 4800
    contour_interval = 60
    levels = np.arange(min_cont, max_cont, contour_interval)
    cf = plt.contourf(lon, lat, data_values, levels=levels, cmap="Blues_r")
    contours = ax.contour(lon, lat, data_values, levels=levels, colors="black")
    plt.clabel(contours, inline=True, fontsize=10)
    for geom in watershed_poly:
        ax.add_geometries(
            [geom], crs=ccrs.PlateCarree(), facecolor="none", edgecolor="red"
        )
    plt.colorbar(cf, label=f"500mb Geopotential Height")
    plt.title(f"{var} @ {formatted_date}")


# year = 2016
# month = '02'
# day = '15'
# hour = '00'
# water_year = 2016

# SOUTH_NORTH_DIM= '[50:1:600]'
# WEST_EAST_DIM= '[550:1:1200]'
# TIME_DIM = '[0:1:0]'

# vars_2d = ['SBCAPE', 'PWAT', 'PREC_ACC_NC', 'SRH03', 'PSFC']
# vars_url_part_2d = ",".join([f"{var}{TIME_DIM}{SOUTH_NORTH_DIM}{WEST_EAST_DIM}" for var in vars_2d])

# constants_url = f'https://thredds.rda.ucar.edu/thredds/dodsC/files/g/ds559.0/INVARIANT/wrfconstants_usgs404.nc?Time[0:1:0],XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},HGT[0:1:0]{SOUTH_NORTH_DIM}{WEST_EAST_DIM}'
# ds_constants = xr.open_dataset(constants_url)
# url_2d = f"https://thredds.rda.ucar.edu/thredds/dodsC/files/g/ds559.0/wy{water_year}/{year}{month}/wrf2d_d01_{year}-{month}-{day}_{hour}:00:00.nc?Time{TIME_DIM},XLAT{SOUTH_NORTH_DIM}{WEST_EAST_DIM},XLONG{SOUTH_NORTH_DIM}{WEST_EAST_DIM},{vars_url_part_2d}"
# ds_2d = xr.open_dataset(url_2d)
