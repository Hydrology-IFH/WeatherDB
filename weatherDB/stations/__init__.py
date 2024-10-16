"""
This module has grouping classes for all the stations of one parameter. E.G. StationsP (or StationsP) groups all the Precipitation Stations available.
Those classes can get used to do actions on all the stations.
"""
from .StationsP import StationsP
from .StationsPD import StationsPD
from .StationsT import StationsT
from .StationsET import StationsET

__all__ = ["StationsP", "StationsPD", "StationsT", "StationsET"]