__author__ = "Max Schmit"
__email__ = "max.schmit@hydrology.uni-freiburg.de"
__copyright__ = "Copyright 2024, Max Schmit"

try:
    from ._version import __version__
except ImportError:
    __version__ = "0.0.0" # not set when running from source

from .utils.logging import remove_old_logs, setup_logging_handlers
from . import station, stations, broker
from .station import StationP, StationPD, StationT, StationET, GroupStation
from .stations import StationsP, StationsPD, StationsT, StationsET, GroupStations
from .config import config
from .broker import Broker


remove_old_logs()
setup_logging_handlers()

__all__ = ["StationP", "StationPD", "StationT", "StationET", "GroupStation",
           "StationsP", "StationsPD", "StationsT", "StationsET", "GroupStations",
           "Broker",
           "station", "stations", "broker", "config"]