"""
This module has a class for every type of station. E.g. StationP (or StationP).
One object represents one Station with one parameter.
This object can get used to get the corresponding timeserie.
There is also a StationGroup class that groups the three parameters precipitation, temperature and evapotranspiration together for one station.
"""
from .StationET import StationET
from .StationP import StationP
from .StationPD import StationPD
from .StationT import StationT

__all__ = ["StationET", "StationP", "StationPD", "StationT"]