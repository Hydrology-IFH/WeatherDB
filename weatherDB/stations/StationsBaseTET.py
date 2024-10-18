# libraries
import logging

from ..db.connections import db_engine
from .StationsBase import StationsBase
from .StationsP import StationsP

# set settings
# ############
__all__ = ["StationsBaseTET"]
log = logging.getLogger(__name__)

# class definition
##################
class StationsBaseTET(StationsBase):
    @db_engine.deco_update_privilege
    def fillup(self, only_real=False, stids="all", **kwargs):
        # create virtual stations if necessary
        if not only_real:
            meta = self.get_meta(
                infos=["Station_id"], only_real=False)
            meta_p = StationsP().get_meta(
                infos=["Station_id"], only_real=False)
            stids_missing = set(meta_p.index.values) - set(meta.index.values)
            if stids != "all":
                stids_missing = set(stids).intersection(stids_missing)
            for stid in stids_missing:
                self._StationClass(stid) # this creates the virtual station

        super().fillup(only_real=only_real, stids=stids, **kwargs)
