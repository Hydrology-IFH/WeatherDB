#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A collection of geometry functions.

Based on `max_fun` package on https://github.com/maxschmi/max_fun
Created by Max Schmit, 2021
"""
# libraries
import numpy as np
from shapely.geometry import Point, LineString
import geopandas as gpd
import rasterio as rio

# functions
def polar_line(center_xy, radius, angle):
    """Create a LineString with polar coodinates.

    Parameters
    ----------
    center_xy : list, array or tuple of int or floats
        The X and Y coordinates of the center.
    radius : int or float
        The radius of the circle.
    angle : int
        The angle of the portion of the circle in degrees.
        0 means east.

    Returns
    -------
    shapely.geometry.LineString
        LineString.
    """
    coords = [center_xy]
    coords.append([
            center_xy[0] + np.cos(np.deg2rad(angle)) * radius,
            center_xy[1] + np.sin(np.deg2rad(angle)) * radius
        ])

    return LineString(coords)

def raster2points(raster_np, transform, crs=None):
    """Polygonize raster array to GeoDataFrame.

    Until now this only works for rasters with one band.

    Parameters
    ----------
    raster_np : np.array
        The imported raster array.
    transform : rio.Affine
        The Affine transformation of the raster.
    crs : str or crs-type, optional
        The coordinate reference system for the raster, by default None

    Returns
    -------
    geopandas.GeoDataFrame
        The raster Data is in the data column.
    """
    mask = ~np.isnan(raster_np[0])
    cols, rows =  mask.nonzero()
    coords = rio.transform.xy(transform, cols, rows)

    geoms = [Point(xy) for xy in list(zip(*coords))]

    return gpd.GeoDataFrame(
        {"data": raster_np[0][mask]},
        geometry=geoms,
        crs=crs)