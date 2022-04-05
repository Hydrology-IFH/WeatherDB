"""
This module has a class for every type of station. E.g. PrecipitationStation (or StationN).
One object represents one Station with one parameter.
This object can get used to get the corresponding timeserie.
There is also a StationGroup class that groups the three parameters precipitation, temperature and evapotranspiration together for one station.
"""
from .stations import (StationsN, StationsND, StationsT, StationsET, GroupStations)

__all__ = ["stations.StationsN", "stations.StationsND", "stations.StationsT", "stations.StationsET", "stations.GroupStations"]