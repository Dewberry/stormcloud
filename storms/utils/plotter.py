from geopandas import GeoSeries
from matplotlib import pyplot as plt
from matplotlib.cm import Spectral_r
import os


def cluster_plot(xdata, cluster_geometry, vmin, vmax, scale_label, multiplier: int = 1, geom=None, png=None):

    fig, ax = plt.subplots()

    fig.set_facecolor("w")
    colormap = Spectral_r
    (xdata["APCP_surface"] * multiplier).plot(
        ax=ax, cmap=colormap, cbar_kwargs={"label": scale_label}, vmin=vmin, vmax=vmax
    )
    if geom is not None:
        GeoSeries(geom, crs="EPSG:4326").plot(ax=ax, facecolor="none", edgecolor="gray", lw=0.7)
    GeoSeries(cluster_geometry, crs="EPSG:4326").plot(ax=ax, facecolor="none", edgecolor="black", lw=1)
    ax.set(title=None, xlabel=None, ylabel=None)
    ax.set_xticks(ax.get_xticks())
    ax.set_xticklabels(ax.get_xticklabels(), rotation=45, ha="right")

    if png is None:
        plt.show()
    else:
        fig.savefig(os.path.join(png))

    fig.clf()
    plt.close(fig)
