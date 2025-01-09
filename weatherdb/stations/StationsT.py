# libraries
import logging

from ..station import StationT
from .StationsBaseTET import StationsBaseTET

# set settings
# ############
__all__ = ["StationsT"]
log = logging.getLogger(__name__)

# class definition
##################

class StationsT(StationsBaseTET):
    """A class to work with and download temperature data for several stations."""
    _StationClass = StationT
    _timeout_raw_imp = 120

    def get_quotient(self, **kwargs):
        raise NotImplementedError("The quotient is not yet implemented for temperature.")
