from boto3 import Session
from datetime import datetime
from math import ceil
import numpy as np
import os
from storms.utils import plotter
from storms.cluster import (
    get_xr_dataset,
    s3_geometry_reader,
    get_atlas14,
)
from storms.transpose import Transposer

session = Session(os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"])


# start = datetime(2010, 4, 30)
start = datetime(2018, 4, 15)
duration = 72
data_type = "precipitation"
aggregate_method = "sum"
domain_uri = "s3://tempest/watersheds/kanawha/kanawha-transpo-area-v01.geojson"
watershed_uri = "s3://tempest/watersheds/kanawha/kanawha-basin.geojson"


transposition_geom = s3_geometry_reader(session, domain_uri)
watershed_geom = s3_geometry_reader(session, watershed_uri)


# get AORC masked by transposition domain
xsum = get_xr_dataset(data_type, start, duration, mask=transposition_geom, aggregate_method=aggregate_method)
transform = xsum.rio.transform()
cellsize_x = abs(transform[0])
cellsize_y = abs(transform[4])
vmax = ceil(np.nanmax(xsum.APCP_surface.to_numpy()))

# get atlas 14 data for normalizing
atlas_14_uri = "s3://tempest/transforms/atlas14/2yr03da/2yr03da.vrt"
xnorm = get_atlas14(atlas_14_uri, xsum.APCP_surface)
norm_arr = xnorm.to_numpy() * 25.4  # convert to mm

# transpose watershed around transposition domain
transposer = Transposer(xsum, watershed_geom, normalized_data=norm_arr)

# get mean ranks
mean_ranks = transposer.ranks("mean", rank_method="min")
mean_transl = transposer.translates[mean_ranks == 1]
mean_geoms = [t.geom(cellsize_x, cellsize_y) for t in mean_transl]
plotter.cluster_plot(xsum, mean_geoms, 0, vmax, "Accumulation (MM)", geom=watershed_geom)


# get sum ranks
sum_ranks = transposer.ranks("sum", rank_method="min")
sum_transl = transposer.translates[sum_ranks == 1]
sum_geoms = [t.geom(cellsize_x, cellsize_y) for t in sum_transl]

plotter.cluster_plot(xsum, sum_geoms, 0, vmax, "Accumulation (MM)", geom=watershed_geom)


# get max ranks (ties)
max_ranks = transposer.ranks("max", rank_method="min")
max_transl = transposer.translates[max_ranks == 1]
max_transl = max_transl[mean_ranks[max_ranks == 1] == mean_ranks[max_ranks == 1].min()]  # tie breaker
max_geoms = [t.geom(cellsize_x, cellsize_y) for t in max_transl]

plotter.cluster_plot(xsum, max_geoms, 0, vmax, "Accumulation (MM)", geom=watershed_geom)


# get norm ranks
norm_ranks = transposer.ranks("normalized_mean", rank_method="min")
norm_transl = transposer.translates[norm_ranks == 1]
norm_geoms = [t.geom(cellsize_x, cellsize_y) for t in norm_transl]

plotter.cluster_plot(xsum, norm_geoms, 0, vmax, "Accumulation (MM)", geom=watershed_geom)
plotter.cluster_plot((xsum / (xnorm * 25.4)), norm_geoms, 0, 1, "Normal Accumulation (MM)", geom=watershed_geom)
