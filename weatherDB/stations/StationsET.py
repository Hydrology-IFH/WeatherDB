# libraries
import logging

from ..station import StationET
from .StationsBaseTET import StationsTETBase

# set settings
# ############
__all__ = ["StationsET"]
log = logging.getLogger(__name__)

# class definition
##################
class StationsET(StationsTETBase):
    """A class to work with and download potential Evapotranspiration (VPGB) data for several stations."""
    _StationClass = StationET
    _timeout_raw_imp = 120