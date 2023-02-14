import logging

logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)

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

    xticks = ax.get_xticks()
    xticks_mask = (xticks >= xdata.longitude.to_numpy().min()) & (xticks <= xdata.longitude.to_numpy().max())
    xtick_labels = [f"{xt:.1f}" for xt in xticks[xticks_mask]]
    ax.set_xticks(xticks[xticks_mask], xtick_labels, rotation=45, ha="center")

    yticks = ax.get_yticks()
    yticks_mask = (yticks >= xdata.latitude.to_numpy().min()) & (yticks <= xdata.latitude.to_numpy().max())
    ytick_labels = [f"{yt:.1f}" for yt in yticks[yticks_mask]]
    ax.set_yticks(yticks[yticks_mask], ytick_labels, va="center")

    if png is None:
        plt.show()
    else:
        fig.savefig(os.path.join(png))

    fig.clf()
    plt.close(fig)
