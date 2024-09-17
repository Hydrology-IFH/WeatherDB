# libraries
import logging

from ..station import StationND
from .StationsN import StationsN
from .StationsBase import StationsBase

# set settings
# ############
__all__ = ["StationsND"]
log = logging.getLogger(__name__)

# class definition
##################

class StationsND(StationsBase):
    """A class to work with and download daily precipitation data for several stations.

    Those stations data are only downloaded to do some quality checks on the 10 minutes data.
    Therefor there is no special quality check and richter correction done on this data.
    If you want daily precipitation data, better use the 10 minutes station class (StationN) and aggregate to daily values.
    """
    _StationClass = StationND
    _StationClass_parent = StationsN
    _timeout_raw_imp = 120
