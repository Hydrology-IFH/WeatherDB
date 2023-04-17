#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A collection of geometry functions to use in the naturwb Webtool app."""

##############################################################################
#               Masterarbeit Uni Freiburg Hydrologie                         #
#                                                                            #
#  Ermittlung einer naturnahen urbanen Wasserbilanz (NatUrWB)                #
#               als Zielvorgabe für deutsche Städte                          #
#                               -                                            #
#              Erstellung eines interaktiven Webtools                        #
#                                                                            #
##############################################################################

__author__ = "Max Schmit"
__copyright__ = "Copyright 2021, Max Schmit"
__version__ = "1.0.1"
__email__ = "maxschm@hotmail.com"

# libraries
import requests
from shapely.geometry import shape
import numpy as np
from shapely.geometry import Polygon, Point, LineString
import geopandas as gpd 
import rasterio as rio

# functions
def geoencode(name, simplified=True):
    """Make a query to Novatim to get the best Polygon.

    Parameters
    ----------
    name : str
        The name of the german town or city to look for.
    simplified : boolean
        Should the geom get simplified?

    Returns
    -------
    GEOSGeometry.Polygon or MultiPolygons
        Simplified Geometry.
    """
    query_answ = requests.get("https://nominatim.openstreetmap.org/search?q=" +
                              name +
                              ",germany&polygon_geojson=1&format=geojson")

    # get the first polygon
    for feature in query_answ.json()["features"]:
        if feature["geometry"]["type"] in ["Polygon", "MultiPolygon"]:
            geom = shape(feature["geometry"])
            break

    if simplified:
        geom = geom.simplify(0.0001)

    return geom

def circle_part(center_xy, radius, start_angle, stop_angle):
    """Create a Portion of a circle as Polygon.

    Parameters
    ----------
    center_xy : list, array or tuple of int or floats
        The X and Y coordinates of the center.
    radius : int or float
        The radius of the circle.
    start_angle : int
        The start angle of the portion of the circle in degrees.
        0 means east.
    stop_angle : int
        The stop angle of the portion of the circle in degrees.
        0 means east.

    Returns
    -------
    shapely.geometry.Polygon
        Polygon of the partion of the circle
    """    
    # switch start/stop angle if necessary
    if start_angle > stop_angle:
        temp = stop_angle
        stop_angle = start_angle 
        start_angle = temp
    if stop_angle - start_angle >= 360:
        return Point(center_xy).buffer(radius)

    x,y = center_xy
    coords = [center_xy]
    for ang in range(start_angle, stop_angle+1, 1):
        coords.append([
            x + np.cos(np.deg2rad(ang)) * radius,
            y + np.sin(np.deg2rad(ang)) * radius
        ])
        
    return Polygon(coords)

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

    if len(cols) == 1:
        geoms = [Point(coords)]
    else:
        geoms = [Point(xy) for xy in list(zip(*coords))]

    return gpd.GeoDataFrame(
        {"data": raster_np[0][mask]}, 
        geometry=geoms, 
        crs=crs)