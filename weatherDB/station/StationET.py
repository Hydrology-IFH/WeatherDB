# libraries
import logging
import sqlalchemy as sa
from sqlalchemy import text as sqltxt
from functools import cached_property

from ..db.connections import db_engine
from ..db.models import MetaET
from .StationBases import StationTETBase

# set settings
# ############
__all__ = ["StationET"]
log = logging.getLogger(__name__)

# class definition
##################
class StationET(StationTETBase):
    """A class to work with and download potential Evapotranspiration (VPGB) data for one station."""

    # common settings
    _MetaModel = MetaET
    _para = "et"
    _para_base = _para
    _para_long = "potential Evapotranspiration"
    _unit = "mm/Tag"
    _decimals = 10

    # cdc dwd parameters
    _ftp_folder_base = ["climate_environment/CDC/derived_germany/soil/daily/"]
    _ftp_zip_regex_prefix = r".*_v2_"
    _cdc_date_col = "Datum"
    _cdc_col_names_imp = ["VPGFAO"]

    # for regionalistaion
    _ma_terms = ["year"]

    # for the fillup
    _fillup_max_dist = 100000

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = str(id)

    @cached_property
    def _table(self):
        return sa.table(
            f"{self.id}_{self._para}",
            sa.column("timestamp", sa.Date),
            sa.column("raw", sa.Integer),
            sa.column("qc", sa.Integer),
            sa.column("filled", sa.Integer),
            sa.column("filled_by", sa.SmallInteger),
            schema="timeseries")

    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp date PRIMARY KEY,
                raw integer NULL DEFAULT NULL,
                qc integer NULL DEFAULT NULL,
                filled integer NULL DEFAULT NULL,
                filled_by smallint NULL DEFAULT NULL
                );
        '''.format(stid=self.id, para=self._para)
        with db_engine.connect() as con:
            con.execute(sqltxt(sql_add_table))
            con.commit()

    def _get_sql_new_qc(self, period):
        # inversion possible?
        do_invers = self.get_meta(infos=["stationshoehe"])>800

        sql_nears = self._get_sql_near_median(
            period=period, only_real=False, add_is_winter=do_invers,
            extra_cols="raw-nbs_median AS diff")

        sql_null_case = f"""(nears.raw > (nears.nbs_median * 2) AND nears.raw > {3*self._decimals})
                            OR ((nears.raw * 4) < nears.nbs_median AND nears.raw > {2*self._decimals})"""
        if do_invers:
            # without inversion
            sql_null_case = "CASE WHEN (winter) THEN "+\
                f"((nears.raw * 4) < nears.nbs_median AND nears.raw > {2*self._decimals}) ELSE "+\
                f"{sql_null_case} END"

        # create sql for new qc
        sql_new_qc = f"""
            WITH nears AS ({sql_nears})
            SELECT
                timestamp,
                (CASE WHEN ({sql_null_case}
                            OR (nears.raw < 0)
                            OR (nears.raw > {20*self._decimals}))
                    THEN NULL
                    ELSE nears."raw" END) as qc
            FROM nears
        """

        return sql_new_qc

    def get_adj(self, **kwargs):
        main_df, adj_df, ma, main_df_tr = super().get_adj(**kwargs)

        # calculate the yearly
        main_df_y = main_df.groupby(main_df_tr.index.year)\
            .sum(min_count=345).mean()

        adj_df["adj"] = (main_df * (ma[0] / main_df_y)).round(1)

        return adj_df
