__author__ = "Max Schmit"
__email__ = "max.schmit@hydrology.uni-freiburg.de"
__copyright__ = "Copyright 2024, Max Schmit"
__version__ = "0.0.45"

from .utils.logging import remove_old_logs, setup_logging_handlers
from . import station, stations, broker
from .station import StationN, StationND, StationT, StationET, GroupStation
from .stations import StationsN, StationsND, StationsT, StationsET, GroupStations
from .config import config

remove_old_logs()
setup_logging_handlers()