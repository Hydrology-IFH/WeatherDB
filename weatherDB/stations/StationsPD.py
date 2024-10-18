# libraries
import logging

from ..station import StationPD
from .StationsP import StationsP
from .StationsBase import StationsBase

# set settings
# ############
__all__ = ["StationsPD"]
log = logging.getLogger(__name__)

# class definition
##################

class StationsPD(StationsBase):
    """A class to work with and download daily precipitation data for several stations.

    Those stations data are only downloaded to do some quality checks on the 10 minutes data.
    Therefor there is no special quality check and richter correction done on this data.
    If you want daily precipitation data, better use the 10 minutes station class (StationP) and aggregate to daily values.
    """
    _StationClass = StationPD
    _timeout_raw_imp = 120
