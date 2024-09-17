"""
This module has grouping classes for all the stations of one parameter. E.G. StationsN (or StationsN) groups all the Precipitation Stations available.
Those classes can get used to do actions on all the stations.
"""
from .StationsN import StationsN
from .StationsND import StationsND
from .StationsT import StationsT
from .StationsET import StationsET

__all__ = ["StationsN", "StationsND", "StationsT", "StationsET"]