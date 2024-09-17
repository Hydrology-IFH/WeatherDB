# libraries
import logging

from ..station import StationT
from .StationsBaseTET import StationsTETBase

# set settings
# ############
__all__ = ["StationsT"]
log = logging.getLogger(__name__)

# class definition
##################

class StationsT(StationsTETBase):
    """A class to work with and download temperature data for several stations."""
    _StationClass = StationT
    _timeout_raw_imp = 120
