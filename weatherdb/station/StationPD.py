# libraries
import logging
import sqlalchemy as sa
from sqlalchemy import text as sqltxt
from functools import cached_property

from ..db.connections import db_engine
from ..utils.dwd import dwd_id_to_str
from ..db.models import MetaPD
from .StationBases import StationPBase, StationCanVirtualBase

# set settings
# ############
__all__ = ["StationPD"]
log = logging.getLogger(__name__)

# class definition
##################
class StationPD(StationPBase, StationCanVirtualBase):
    """A class to work with and download daily precipitation data for one station.

    Those station data are only downloaded to do some quality checks on the 10 minute data.
    Therefor there is no special quality check and richter correction done on this data.
    If you want daily precipitation data, better use the 10 minutes station(StationP) and aggregate to daily values."""

    # common settings
    _MetaModel = MetaPD
    _para = "p_d"
    _para_base = "p"
    _para_long = "daily Precipitation"
    _unit = "mm/day"
    _valid_kinds = {"raw", "filled", "filled_by"}
    _best_kind = "filled"

    # cdc dwd parameters
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/",
        "climate_environment/CDC/observations_germany/climate/daily/more_precip/"]
    _cdc_col_names_imp = ["RSK"]
    _db_col_names_imp = ["raw"]

    # timestamp configurations
    _tstp_format_db = "%Y%m%d"
    _tstp_format_human = "%Y-%m-%d"
    _tstp_dtype = "date"
    _interval = "1 day"

    # aggregation
    _min_agg_to = "day"

    # methods from the base class that should not be active for this class
    quality_check = property(doc='(!) Disallowed inherited')
    last_imp_quality_check = property(doc='(!) Disallowed inherited')
    get_corr = property(doc='(!) Disallowed inherited')
    get_adj = property(doc='(!) Disallowed inherited')
    get_qc = property(doc='(!) Disallowed inherited')

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = dwd_id_to_str(id)

    def _download_raw(self, zipfiles):
        df_all, max_hist_tstp = super()._download_raw(zipfiles)

        # fill RSK with values from RS if not given
        if "RS" in df_all.columns and "RSK" in df_all.columns:
            mask = df_all["RSK"].isna()
            df_all.loc[mask, "RSK"] = df_all.loc[mask, "RS"]
        elif "RS" in df_all.columns:
            df_all["RSK"] = df_all["RS"]

        return df_all, max_hist_tstp

    @cached_property
    def _table(self):
        return sa.table(
            f"{self.id}_{self._para}",
            sa.column("timestamp", sa.Date),
            sa.column("raw", sa.Integer),
            sa.column("filled", sa.Integer),
            sa.column("filled_by", sa.SmallInteger),
            schema="timeseries")

    @db_engine.deco_create_privilege
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp date PRIMARY KEY,
                raw int4,
                filled int4,
                filled_by int2
            );
        '''.format(stid=self.id, para=self._para)
        with db_engine.connect() as con:
            con.execute(sqltxt(sql_add_table))
            con.commit()

