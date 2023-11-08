import logging

logging.getLogger("matplotlib.font_manager").setLevel(logging.WARNING)

import os

from matplotlib import patches
from matplotlib import pyplot as plt
from matplotlib.cm import Spectral_r
from numpy import column_stack


def geom_to_patches(geom, lw, facecolor, edgecolor):
    patches_list = []
    if isinstance(geom, list) or isinstance(geom, tuple):
        for _geom in geom:
            if _geom.geom_type == "MultiPolygon":
                for g in _geom.geoms:
                    patches_list.append(
                        patches.Polygon(
                            column_stack(g.exterior.coords.xy),
                            lw=lw,
                            facecolor=facecolor,
                            edgecolor=edgecolor,
                        )
                    )
            else:
                patches_list.append(
                    patches.Polygon(
                        column_stack(_geom.exterior.coords.xy),
                        lw=lw,
                        facecolor=facecolor,
                        edgecolor=edgecolor,
                    )
                )
    else:
        if geom.geom_type == "MultiPolygon":
            for g in geom.geoms:
                patches_list.append(
                    patches.Polygon(
                        column_stack(g.exterior.coords.xy),
                        lw=lw,
                        facecolor=facecolor,
                        edgecolor=edgecolor,
                    )
                )
        else:
            patches_list.append(
                patches.Polygon(
                    column_stack(geom.exterior.coords.xy),
                    lw=lw,
                    facecolor=facecolor,
                    edgecolor=edgecolor,
                )
            )

    return patches_list


def cluster_plot(
    xdata,
    cluster_geometry,
    vmin,
    vmax,
    scale_label,
    multiplier: int = 1,
    geom=None,
    png=None,
    figsize: tuple = (5, 5),
):
    fig, ax = plt.subplots(figsize=figsize)

    fig.set_facecolor("w")

    colormap = Spectral_r
    (xdata["APCP_surface"] * multiplier).plot(
        ax=ax, cmap=colormap, cbar_kwargs={"label": scale_label}, vmin=vmin, vmax=vmax
    )

    if geom is not None:
        for patch in geom_to_patches(geom, 0.7, "none", "gray"):
            ax.add_patch(patch)

    for patch in geom_to_patches(cluster_geometry, 1, "none", "black"):
        ax.add_patch(patch)

    ax.set(title=None, xlabel=None, ylabel=None)

    if png is None:
        plt.show()
    else:
        fig.savefig(os.path.join(png), bbox_inches="tight")

    fig.clf()
    plt.close(fig)
