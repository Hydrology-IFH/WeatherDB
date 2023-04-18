import os

__author__ = "Max Schmit"
__email__ = "max.schmit@hydrology.uni-freiburg.de"
__copyright__ = "Copyright 2023, Max Schmit"
__version__ = "0.0.25"

if not ("WEATHERDB_MODULE_INSTALLING" in os.environ \
        and os.environ["WEATHERDB_MODULE_INSTALLING"]=="True"):
    from .wdb_logging import remove_old_logs, setup_file_logging

    remove_old_logs()

    # import classes
    from . import station, stations
    from .station import StationN, StationND, StationT, StationET, GroupStation
    from .stations import StationsN, StationsND, StationsT, StationsET, GroupStations
    try:
        from . import broker
    except PermissionError:
        pass