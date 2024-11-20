# libraries
import logging
import sqlalchemy as sa
from sqlalchemy import text as sqltxt
from functools import cached_property

from ..db.connections import db_engine
from ..utils.dwd import dwd_id_to_str
from ..db.models import MetaT
from .StationBases import StationTETBase

# set settings
# ############
__all__ = ["StationT"]
log = logging.getLogger(__name__)

# class definition
##################
class StationT(StationTETBase):
    """A class to work with and download temperaure data for one station."""

    # common settings
    _MetaModel = MetaT
    _para = "t"
    _para_base = _para
    _para_long = "Temperature"
    _unit = "°C"
    _decimals = 10
    _valid_kinds = {"raw", "raw_min", "raw_max", "qc",
                    "filled", "filled_min", "filled_max", "filled_by"}

    # cdc dwd parameters
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/"]
    _cdc_date_col = "MESS_DATUM"
    _cdc_col_names_imp = ["TMK", "TNK", "TXK"]
    _db_col_names_imp = ["raw", "raw_min", "raw_max"]

    # aggregation
    _agg_fun = "avg"

    # for regionalistaion
    _ma_terms = ["year"]
    _coef_sign = ["-", "+"]

    # # for the fillup
    _filled_by_n = 5
    _fillup_max_dist = 100e3


    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = dwd_id_to_str(id)

    @cached_property
    def _table(self):
        return sa.table(
            f"{self.id}_{self._para}",
            sa.column("timestamp", sa.Date),
            sa.column("raw", sa.Integer),
            sa.column("raw_min", sa.Integer),
            sa.column("raw_max", sa.Integer),
            sa.column("qc", sa.Integer),
            sa.column("filled", sa.Integer),
            sa.column("filled_min", sa.Integer),
            sa.column("filled_max", sa.Integer),
            sa.column("filled_by", sa.SmallInteger),
            schema="timeseries")

    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = f'''
            CREATE TABLE IF NOT EXISTS timeseries."{self.id}_{self._para}"  (
                timestamp date PRIMARY KEY,
                raw integer NULL DEFAULT NULL,
                raw_min integer NULL DEFAULT NULL,
                raw_max integer NULL DEFAULT NULL,
                qc integer NULL DEFAULT NULL,
                filled integer NULL DEFAULT NULL,
                filled_min integer NULL DEFAULT NULL,
                filled_max integer NULL DEFAULT NULL,
                filled_by smallint[{self._filled_by_n}] NULL DEFAULT NULL
                );
        '''
        with db_engine.connect() as con:
            con.execute(sqltxt(sql_add_table))
            con.commit()

    def _get_sql_new_qc(self, period):
        # inversion possible?
        do_invers = self.get_meta(infos=["stationshoehe"])>800

        sql_nears = self._get_sql_near_median(
            period=period, only_real=False, add_is_winter=do_invers,
            extra_cols="raw-nbs_median AS diff")

        if do_invers:
            # without inversion
            sql_null_case = "CASE WHEN (winter) THEN "+\
                f"diff < {-5 * self._decimals} ELSE "+\
                f"ABS(diff) > {5 * self._decimals} END "+\
                f"OR raw < {-50 * self._decimals} OR raw > {50 * self._decimals}"
        else:
            # with inversion
            sql_null_case = f"ABS(diff) > {5 * self._decimals}"

        # create sql for new qc
        sql_new_qc = f"""
            WITH nears AS ({sql_nears})
            SELECT
                timestamp,
                (CASE WHEN ({sql_null_case})
                    THEN NULL
                    ELSE nears."raw"
                    END) as qc
            FROM nears
        """

        return sql_new_qc

    @db_engine.deco_update_privilege
    def _sql_fillup_extra_dict(self, **kwargs):
        # additional parts to calculate the filling of min and max
        fillup_extra_dict = super()._sql_fillup_extra_dict(**kwargs)
        sql_array_init = "ARRAY[{0}]".format(
            ", ".join(["NULL::smallint"] * self._filled_by_n))
        fillup_extra_dict.update({
            "extra_new_temp_cols": "raw_min AS filled_min, raw_max AS filled_max," +
                        f"{sql_array_init} AS nb_min, {sql_array_init} AS nb_max,",
            "extra_cols_fillup_calc": "filled_min=round(nb.raw_min + %3$s, 0)::int, " +
                                      "filled_max=round(nb.raw_max + %3$s, 0)::int, ",
            "extra_cols_fillup": "filled_min = new.filled_min, " +
                                 "filled_max = new.filled_max, ",
            "extra_fillup_where": ' OR ts."filled_min" IS DISTINCT FROM new."filled_min"' +
                                  ' OR ts."filled_max" IS DISTINCT FROM new."filled_max"',
            "extra_exec_cols": "nb_max[{i}]=round(nb.raw_max + %3$s, 0)::int,"+
                               "nb_min[{i}]=round(nb.raw_min + %3$s, 0)::int,",
            "extra_after_loop_extra_col": """,
                filled_min=(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
                            FROM unnest(nb_min) as T(v)),
                filled_max=(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
                            FROM unnest(nb_max) as T(v))"""})
        return fillup_extra_dict

    def get_multi_annual_raster(self):
        mas = super().get_multi_annual_raster()
        if mas is not None:
            return [ma / 10 for ma in mas]
        else:
            return None

    def get_adj(self, **kwargs):
        main_df, adj_df, ma, main_df_tr = super().get_adj(**kwargs)

        # calculate the yearly
        main_df_y = main_df.groupby(main_df_tr.index.year)\
            .mean().mean()

        adj_df["adj"] = (main_df + (ma[0] - main_df_y)).round(1)

        return adj_df

    def get_quotient(self, **kwargs):
        raise NotImplementedError("The quotient is not yet implemented for temperature.")