# libraries
import itertools
import logging
import re
import time
from datetime import datetime, timedelta, date
from pathlib import Path
import warnings
import numpy as np
import pandas as pd
from sqlalchemy.exc import OperationalError
from sqlalchemy import text as sqltxt
import sqlalchemy as sa
import rasterio as rio
import shapely.wkb
import shapely.ops
import pyproj
from rasterstats import zonal_stats
import textwrap
from functools import cached_property

from ..db.connections import db_engine
from ..utils.dwd import get_cdc_file_list, get_dwd_file
from ..utils.TimestampPeriod import TimestampPeriod
from ..config import config
from ..db.models import StationMARaster, MetaBase, RawFiles
from .constants import AGG_TO
from ..db.queries.get_quotient import _get_quotient

# set settings
# ############
__all__ = ["StationBase"]
log = logging.getLogger(__name__)

# class definitions
###################

class StationBase:
    """This is the Base class for one Station.
    It is not working on it's own, because those parameters need to get defined in the real classes
    """
    # those parameters need to get defined in the real classes:
    ################################################################

    # common settings
    # ---------------
    # The sqlalchemy model of the meta table
    _MetaModel = MetaBase
    # The parameter string "p", "t", "et" or "p_d"
    _para = None
    # The base parameter, without the "_d" suffix
    _para_base = None
    # The parameter as a long descriptive string
    _para_long = None
    # The Unit as str
    _unit = "None"
    # the factor to change data to integers for the database
    _decimals = 1
    # the kinds that should not get multiplied with the amount of decimals, e.g. "qn"
    _kinds_not_decimal = ["qn", "filled_by", "filled_share"]
    # The valid kinds to use. Must be a column in the timeseries tables.
    _valid_kinds = {"raw", "qc",  "filled", "filled_by"}
    # the kind that is best for simulations
    _best_kind = "filled"

    # cdc dwd parameters
    # ------------------
    # the base folder on the CDC-FTP server
    _ftp_folder_base = ["None"]
    # a regex prefix to the default search regex to find the zip files on the CDC-FTP server
    _ftp_zip_regex_prefix = None
    # The name of the date column on the CDC server
    _cdc_date_col = None
    # the names of the CDC columns that get imported
    _cdc_col_names_imp = [None]
    # the corresponding column name in the DB of the raw import
    _db_col_names_imp = ["raw"]

    # timestamp configurations
    # ------------------------
    # The format string for the strftime for the database to be readable
    _tstp_format_db = None
    # the format of the timestamp to be human readable
    _tstp_format_human = "%Y-%m-%d %H:%M"
    # the postgresql data type of the timestamp column, e.g. "date" or "timestamp"
    _tstp_dtype = None
    # The interval of the timeseries e.g. "1 day" or "10 min"
    _interval = None

    # aggregation
    # -----------
    # Similar to the interval, but same format ass in AGG_TO
    _min_agg_to = None
    # the sql aggregating function to use
    _agg_fun = "sum"

    # for regionalistaion
    # -------------------
    # the key names of the band names in the config file, without prefix "BAND_".
    # Specifies the term in the db to use to calculate the coefficients, 2 values: wi/so or one value:yearly
    _ma_terms = []
    # The sign to use to calculate the coefficient (first element) and to use the coefficient (second coefficient).
    _coef_sign = ["/", "*"]
    # The multi annual raster to use to calculate the multi annual values
    _ma_raster_key = "dwd" # section name in the config file (data:rasters:...)

    # for the fillup
    # --------------
    # How many neighboring stations are used for the fillup procedure
    _filled_by_n = 1
    # The maximal distance in meters to use to get neighbor stations for the fillup. Only relevant if multiple stations are considered for fillup.
    _fillup_max_dist = None

    def __init__(self, id, _skip_meta_check=False):
        """Create a Station object.

        Parameters
        ----------
        id : int
            The stations ID.
        _skip_meta_check : bool, optional
            Should the check if the station is in the database meta file get skiped.
            Pay attention, when skipping this, because it can lead to problems.
            This is for computational reasons, because it makes the initialization faster.
            Is used by the stations classes, because the only initialize objects that are in the meta table.
            The default is False

        Raises
        ------
        NotImplementedError
            If the class is initiated with a station ID that is not in the database.
            To prevent this error, set _skip_meta_check=True.
        """
        if type(self) is StationBase:
            raise NotImplementedError("""
            The StationBase is only a wrapper class an is not working on its own.
            Please use StationP, StationT or StationET instead""")
        self.id = int(id)
        self.id_str = str(id)

        if isinstance(self._ftp_folder_base, str):
            self._ftp_folder_base = [self._ftp_folder_base]

        # create ftp_folders in order of importance
        self._ftp_folders = list(itertools.chain(*[
            [base + "historical/", base + "recent/"]
            for base in self._ftp_folder_base]))

        self._db_unit = " ".join([str(self._decimals), self._unit])
        if not _skip_meta_check:
            self._check_isin_meta()

        # initiate the dictionary to store the last checked periods
        self._cached_periods = dict()

    @property
    def _ma_raster_conf(self):
        return config[f"data:rasters:{self._ma_raster_key}"]

    def _check_isin_meta(self):
            if self.isin_meta():
                return True
            else:
                raise NotImplementedError("""
                    The given {para_long} station with id {stid}
                    is not in the corresponding meta table in the DB""".format(
                    stid=self.id, para_long=self._para_long
                ))

    @classmethod
    def _check_kind(cls, kind, valids=None):
        """Check if the given kind is valid.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        valids : set of str, optional
            Additional kinds that are valid.
            This is used to add additional kinds that are valid.
            The default is an empty set.

        Raises
        ------
        NotImplementedError
            If the given kind is not valid.
        ValueError
            If the given kind is not a string.
        """
        # check valids
        if valids is None:
            valids = cls._valid_kinds

        # check kind
        if not isinstance(kind, str):
            raise ValueError("The given kind is not a string.")

        if kind == "best":
            kind = cls._best_kind

        if kind not in valids:
            raise NotImplementedError("""
                The given kind "{kind}" is not a valid kind.
                Must be one of "{valid_kinds}"
                """.format(
                kind=kind,
                valid_kinds='", "'.join(valids)))

        return kind

    @classmethod
    def _check_kind_tstp_meta(cls, kind):
        """Check if the kind has a timestamp from and until in the meta table."""
        if kind != "last_imp":
            kind = cls._check_kind(kind)

        # compute the valid kinds if not already done
        if not hasattr(cls, "_valid_kinds_tstp_meta"):
            cls._valid_kinds_tstp_meta = ["last_imp"]
            for vk in cls._valid_kinds:
                if vk in {"raw", "qc", "filled", "corr"}:
                    cls._valid_kinds_tstp_meta.append(vk)

        if kind not in cls._valid_kinds_tstp_meta:
            raise NotImplementedError("""
                The given kind "{kind}" is not a valid kind.
                Must be one of "{valid_kinds}"
                """.format(
                kind=kind,
                valid_kinds='", "'.join(cls._valid_kinds_tstp_meta)))

        return kind

    @classmethod
    def _check_kinds(cls, kinds, valids=None):
        """Check if the given kinds are valid.

        Parameters
        ----------
        kinds :  list of str or str
            A list of the data kinds to check if they are valid kinds for this class implementation.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
        valids : set of str or None, optional
            The kinds that are valid.
            This is used to check the kins against.
            If None then the default valids are used. (cls._valid_kinds)
            The default is None.

        Raises
        ------
        NotImplementedError
            If the given kind is not valid.
        ValueError
            If the given kind is not a string.

        Returns
        -------
        kinds: list of str
            returns a list of strings of valid kinds
        """
        # check valids
        if valids is None:
            valids = cls._valid_kinds

        # check kinds
        if isinstance(kinds, str):
            kinds = [kinds]
        else:
            kinds = kinds.copy() # because else the original variable is changed

        for i, kind_i in enumerate(kinds):
            if kind_i not in valids:
                kinds[i] = cls._check_kind(kind_i, valids)
        return kinds

    def _check_period(self, period, kinds, nas_allowed=False):
        """Correct a given period to a valid format.

        If the given Timestamp is none the maximum or minimum possible is given.

        Parameters
        ----------
        period : tuple or list of datetime.datetime or None, optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is False.

        Returns
        -------
        list with 2 datetime.datetime
            The minimum and maximum Timestamp.
        """
        # check if period gor recently checked
        self._clean_cached_periods()
        cache_key = str((kinds, period, nas_allowed))
        if cache_key in self._cached_periods:
            return self._cached_periods[cache_key]["return"]

        # remove filled_by kinds
        if "filled_by" in kinds:
            kinds = kinds.copy()
            kinds.remove("filled_by")
            if len(kinds)==0:
                nas_allowed=True

        # get filled period or max period
        max_period = self.get_max_period(kinds=kinds, nas_allowed=nas_allowed)

        # check if filled_period is empty and throw error
        if max_period.is_empty():
            raise ValueError(
                "No maximum period was found for the {para_long} Station with ID {stid} and kinds '{kinds}'."
                .format(
                    para_long=self._para_long, stid=self.id, kinds="', '".join(kinds)))

        # get period if None providen
        if not isinstance(period, TimestampPeriod):
            period = TimestampPeriod(*period)
        else:
            period = period.copy()

        # do additional period checks in subclasses
        period = self._check_period_extra(period)

        # compare with filled_period
        if period.is_empty():
            period = max_period
        else:
            period = period.union(
                max_period,
                how="inner")

        # save for later
        self._cached_periods.update({
            cache_key: {
                "time": datetime.now(),
                "return": period}})

        return period

    @staticmethod
    def _check_period_extra(period):
        """Additional checks on period to define in subclasses"""
        return period

    def _check_agg_to(self, agg_to):
        agg_to_valid = list(AGG_TO.keys())
        if agg_to not in agg_to_valid:
            raise ValueError(
                "The given agg_to Parameter \"{agg_to}\" is not a valid aggregating period. Please use one of:\n{agg_valid}".format(
                    agg_to=agg_to,
                    agg_valid=", ".join([str(item) for item in agg_to_valid])
                ))
        if agg_to_valid.index(agg_to) <= agg_to_valid.index(self._min_agg_to):
            return None
        else:
            return agg_to

    def _check_df_raw(self, df):
        """This is an empty function to get implemented in the subclasses if necessary.

        It applies extra checkups, like adjusting the timezone on the downloaded raw timeseries and returns the dataframe."""
        # add Timezone as UTC
        df.index = df.index.tz_localize("UTC")

        return df

    def _clean_cached_periods(self):
        time_limit = datetime.now() - timedelta(minutes=1)
        for key in list(self._cached_periods):
            if self._cached_periods[key]["time"] < time_limit:
                self._cached_periods.pop(key)

    @db_engine.deco_update_privilege
    def _check_ma(self):
        if not self.isin_ma():
            self.update_ma_raster()

    @db_engine.deco_create_privilege
    def _check_isin_db(self):
        """Check if the station has already a timeserie and if not create one.
        """
        if not self.isin_db():
            self._create_timeseries_table()

    @db_engine.deco_update_privilege
    def _check_min_date(self, **kwargs):
        """Check if the station has already a timeserie and if not create one.
        """
        min_dt_config = config.get_datetime("weatherdb", "min_date")
        with db_engine.connect() as con:
            # get minimal timeseries timestamp
            try:
                min_dt_ts = con.execute(
                        sa.select(sa.func.min(self._table.c.timestamp))
                    ).scalar()
            except sa.exc.ProgrammingError as e:
                if "relation" in str(e) and "does not exist" in str(e):
                    min_dt_ts = None
                else:
                    raise e
            if min_dt_ts is None:
                return None
            if isinstance(min_dt_ts, date):
                min_dt_ts = datetime.combine(min_dt_ts, datetime.min.time())
            min_dt_ts = min_dt_ts.replace(tzinfo=min_dt_config.tzinfo)

            # compare to config min_date and correct timeseries
            if min_dt_ts < min_dt_config:
                log.debug(f"The Station{self._para}({self.id})'s minimum timestamp of {min_dt_ts} is below the configurations min_date of {min_dt_config.date()}. The timeserie will be reduced to the configuration value.")
                con.execute(
                    sa.delete(self._table)
                    .where(self._table.c.timestamp < min_dt_config)
                )
                con.commit()
            elif min_dt_ts > min_dt_config:
                log.debug(f"The Station{self._para}({self.id})'s minimum timestamp of {min_dt_ts} is above the configurations min_date of {min_dt_config.date()}. The timeserie will be expanded to the configuration value and the raw_files for this station are getting deleted, to reload all possible dwd data.")
                zipfiles = self.get_zipfiles(
                    only_new=False,
                    ftp_file_list=kwargs.get("ftp_file_list", None))
                con.execute(sa.delete(RawFiles.__table__)\
                    .where(sa.and_(
                        RawFiles.filepath.in_(zipfiles.index.to_list()),
                        RawFiles.parameter == self._para)))
                con.commit()
                self._expand_timeserie_to_period()

    @property
    def _ma_raster_bands(self):
        """Get the raster bands of the stations multi annual raster file."""
        return [self._ma_raster_conf[key]
                for key in self._ma_raster_band_conf_keys]

    @property
    def _ma_raster_band_conf_keys(self):
        """Get the raster band keys for the station. E.g. P_WIHY"""
        return [f"band_{self._para_base}_{term}"
                for term in self._ma_terms]

    @property
    def _ma_raster_factors(self):
        """Get the factor to convert the raster values to the real unit e.g. °C or mm."""
        return [float(self._ma_raster_conf.get(key, 1))
                for key in self._ma_raster_factor_conf_keys]

    @property
    def _ma_raster_factor_conf_keys(self):
        """Get the raster band keys for the station. E.g. P_WIHY"""
        return [f"factor_{self._para_base}_{term}"
                for term in self._ma_terms]

    @cached_property
    def _table(self):
        raise NotImplementedError("The table property is not implemented in the base class.")

    @db_engine.deco_create_privilege
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        pass

    @db_engine.deco_update_privilege
    def _expand_timeserie_to_period(self):
        """Expand the timeserie to the complete possible time range"""
        # The interval of 9h and 30 seconds is due to the fact, that the fact that t and et data for the previous day is only updated around 9 on the following day
        # the 10 minutes interval is to get the previous day and not the same day
        sql = """
            WITH whole_ts AS (
                SELECT generate_series(
                    '{min_date} 00:00'::{tstp_dtype},
                    (SELECT
                        LEAST(
                            date_trunc(
                                'day',
                                min(start_tstp_last_imp) - '9h 30min'::INTERVAL
                            ) - '10 min'::INTERVAL,
                            min(CASE WHEN parameter='p' THEN max_tstp_last_imp
                                     ELSE max_tstp_last_imp + '23h 50min'::INTERVAL
                                END))
                    FROM parameter_variables)::{tstp_dtype},
                    '{interval}'::INTERVAL)::{tstp_dtype} AS timestamp)
            INSERT INTO timeseries."{stid}_{para}"(timestamp)
                (SELECT wts.timestamp
                FROM whole_ts wts
                LEFT JOIN timeseries."{stid}_{para}" ts
                    ON ts.timestamp=wts.timestamp
                WHERE ts.timestamp IS NULL);
        """.format(
            stid=self.id,
            para=self._para,
            tstp_dtype=self._tstp_dtype,
            interval=self._interval,
            min_date=config.get("weatherdb", "min_date"))

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    @db_engine.deco_update_privilege
    def _update_db_timeserie(self, df, kinds):
        """Update the timeseries table on the database with new DataFrame.

        Parameters
        ----------
        df : pandas.Series of integers
            A Serie with a DatetimeIndex and the values to update in the Database.
            The values need to be in the database unit. So you might have to multiply your values with self._decimals and convert to integers.
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled".
            For the precipitation also "qn" and "corr" are valid.

        Raises
        ------
        NotImplementedError
            If the given kind is not valid.
        ValueError
            If the given kind is not a string.
        """
        # check kinds
        kinds = self._check_kinds(kinds)

        # check if df is empty
        if len(df) == 0:
            log.debug(("The _update_db_timeserie method got an empty df " +
                    "for the {para_long} Station with ID {stid}"
                    ).format(
                para_long=self._para_long,
                stid=self.id))
            return None
        else:
            self._create_timeseries_table()

        with db_engine.connect() as con:
            # create groups of 1000 values to insert
            groups = np.array_split(df.index, (len(df)//1000)+1)
            for group in groups:
                df_i = df.loc[group]

                # make insert statement
                values_all = [
                    ind.strftime("('%Y%m%d %H:%M', ") + ", ".join(pair) + ")"
                    for ind, pair in zip(df_i.index, df_i.values.astype(str))]
                values = ", ".join(values_all)
                values = re.sub(r"(nan)|(<NA>)", "NULL", values)
                sql_insert = '''
                    INSERT INTO timeseries."{stid}_{para}"(timestamp, "{kinds}")
                    Values {values}
                    ON CONFLICT (timestamp) DO UPDATE SET
                '''.format(
                    stid=self.id, para=self._para,
                    kinds='", "'.join(kinds), values=values)

                for kind_i in kinds:
                    sql_insert += '"{kind}" = EXCLUDED."{kind}",'\
                        .format(kind=kind_i)
                sql_insert = sql_insert[:-1] + ";"

                # run sql command
                con.execute(sqltxt(sql_insert))
            con.commit()

    @db_engine.deco_delete_privilege
    def _drop(self, why="No reason given"):
        """Drop this station from the database. (meta table and timeseries)
        """
        why=why.replace("'", "''")
        sql = f"""
            DROP TABLE IF EXISTS timeseries."{self.id}_{self._para}";
            DELETE FROM meta_{self._para} WHERE station_id={self.id};
            DELETE FROM station_ma_raster WHERE station_id={self.id} and parameter='{self._para}';
            DELETE FROM station_ma_timeseries WHERE station_id={self.id} and parameter='{self._para}';
            INSERT INTO dropped_stations(station_id, parameter, why, timestamp)
            VALUES ('{self.id}', '{self._para}', '{why}', NOW())
            ON CONFLICT (station_id, parameter)
                DO UPDATE SET
                    why = EXCLUDED.why,
                    timestamp = EXCLUDED.timestamp;
        """

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()
        log.debug(
            f"The {self._para_long} Station with ID {self.id} got dropped from the database, because \"{why}\".")

    @db_engine.deco_update_privilege
    def _update_meta(self, cols, values):
        sets = []
        for col, value in zip(cols, values):
            sets.append(
                "{col}='{value}'".format(
                    col=col, value=value))

        sql_update = """
            UPDATE meta_{para}
            SET {sets}
            WHERE station_id={stid};
        """.format(
            stid=self.id,
            para=self._para,
            sets=", ".join(sets)
        )

        with db_engine.connect() as con:
            con.execute(sqltxt(sql_update))
            con.commit()

    @db_engine.deco_all_privileges
    def _execute_long_sql(self, sql, description="treated"):
        done = False
        attempts = 0
        re_comp = re.compile("(the database system is in recovery mode)" +
                             "|(SSL SYSCALL error: EOF detected)" + # login problem due to recovery mode
                             "|(SSL connection has been closed unexpectedly)" + # sudden logoff
                             "|(the database system is shutting down)") # to test the procedure by stoping postgresql
        # execute until done
        while not done:
            attempts += 1
            try:
                with db_engine.connect() as con:
                    con.execute(sqltxt(sql))
                    con.commit()
                done = True
            except OperationalError as err:
                log_msg = ("There was an operational error for the {para_long} Station (ID:{stid})" +
                        "\nHere is the complete error:\n" + str(err)).format(
                    stid=self.id, para_long=self._para_long)
                if any(filter(re_comp.search, err.args)):
                    if attempts > 10:
                        log.error(
                            log_msg +
                            "\nBecause there were already too many attempts, the execution of this process got completely stopped.")
                        break
                    else:
                        log.debug(
                            log_msg +
                            "\nThe execution is stopped for 10 minutes and then redone.")
                        time.sleep(60*10)

        # log
        if done:
            log.info(
                "The {para_long} Station ({stid}) got successfully {desc}.".format(
                    stid=self.id,
                    para_long=self._para_long,
                    desc=description))
        else:
            raise Exception(
                "The {para_long} Station ({stid}) could not get {desc}.".format(
                    stid=self.id,
                    para_long=self._para_long,
                    desc=description)
            )

    @db_engine.deco_update_privilege
    def _set_is_real(self, state=True):
        sql = """
            UPDATE meta_{para}
            SET is_real={state}
            WHERE station_id={stid};
        """.format(stid=self.id, para=self._para, state=state)

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))

    def isin_db(self):
        """Check if Station is already in a timeseries table.

        Returns
        -------
        bool
            True if Station has a table in DB, no matter if it is filled or not.
        """

        sql = """
            select '{stid}_{para}' in (
                select table_name
                from information_schema.columns
                where table_schema='timeseries');
            """.format(para=self._para, stid=self.id)
        with db_engine.connect() as con:
            result = con.execute(sqltxt(sql)).first()[0]

        return result

    def isin_meta(self):
        """Check if Station is already in the meta table.

        Returns
        -------
        bool
            True if Station is in meta table.
        """
        with db_engine.connect() as con:
            result = con.execute(sqltxt("""
            SELECT EXISTS(SELECT station_id FROM meta_{para} WHERE station_id={stid});
            """.format(stid=self.id, para=self._para)))
        return result.first()[0]

    def isin_ma(self):
        """Check if Station is already in the multi annual table.

        Returns
        -------
        bool
            True if Station is in multi annual table.
        """
        sql_select = sa.exists()\
            .where(sa.and_(
                StationMARaster.station_id == self.id,
                StationMARaster.raster_key == self._ma_raster_key,
                StationMARaster.parameter == self._para_base,
                StationMARaster.term.in_(self._ma_terms),
                StationMARaster.value.isnot(None)))
        with db_engine.session() as session:
            return session.query(sql_select).scalar()

    def is_virtual(self):
        """Check if the station is a real station or only a virtual one.

        Real means that the DWD is measuring here.
        Virtual means, that there are no measurements here, but the station got created to have timeseries for every parameter for every precipitation station.

        Returns
        -------
        bool
            true if the station is virtual, false if it is real.
        """
        return not self.is_real()

    def is_real(self):
        """Check if the station is a real station or only a virtual one.

        Real means that the DWD is measuring here.
        Virtual means, that there are no measurements here, but the station got created to have timeseries for every parameter for every precipitation station.

        Returns
        -------
        bool
            true if the station is real, false if it is virtual.
        """
        sql = """
            SELECT is_real
            FROM meta_{para}
            WHERE station_id= {stid}
        """.format(stid=self.id, para=self._para)
        with db_engine.connect() as con:
            res = con.execute(sqltxt(sql))
        return res.first()[0]

    def is_last_imp_done(self, kind):
        """Is the last import for the given kind already worked in?


        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "best".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.

        Returns
        -------
        bool
            True if the last import of the given kind is already treated.
        """

        kind = self._check_kind(kind)
        sql = """
            SELECT last_imp_{kind}
            FROM meta_{para}
            WHERE station_id = {stid}
        """.format(stid=self.id, para=self._para, kind=kind)

        with db_engine.connect() as con:
            res = con.execute(sqltxt(sql))

        return res.first()[0]

    @db_engine.deco_update_privilege
    def update_period_meta(self, kind, **kwargs):
        """Update the time period in the meta file.

        Compute teh filled period of a timeserie and save in the meta table.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "corr" are valid.
        **kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.
        """
        kind = self._check_kind_tstp_meta(kind)
        period = self.get_filled_period(kind=kind)

        sql = """
            UPDATE meta_{para}
            SET {kind}_from={min_tstp},
                {kind}_until={max_tstp}
            WHERE station_id={stid};
        """.format(
            stid=self.id, para=self._para,
            kind=kind,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format_db))
        )

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    @db_engine.deco_update_privilege
    def update_ma_raster(self, skip_if_exist=True, drop_when_error=True, **kwargs):
        """Update the multi annual values in the station_ma_raster table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip the update if the stations multi annual data is already in the table.
            The default is True.
        drop_when_error : bool, optional
            Drop the station from the database if there is an error.
            The default is True.
        **kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.
        """
        if skip_if_exist and self.isin_ma():
            return None

        # get multi annual raster values, starting at point location up to 1000m
        dist = -50
        new_mas = None
        ma_raster_bands = self._ma_raster_bands

        while (new_mas is None or (new_mas is not None and not np.any(~np.isnan(new_mas)))) \
               and dist <= 1000:
            dist += 50
            new_mas = self._get_raster_value(
                raster_conf=self._ma_raster_conf,
                bands=ma_raster_bands,
                dist=dist)

        # write to station_ma_raster table
        if new_mas is not None and np.any(~np.isnan(new_mas)):
            # convert from raster unit to db unit
            new_mas = [int(np.round(val * fact * self._decimals))
                       for val, fact in zip(new_mas, self._ma_raster_factors)]

            # upload in database
            with db_engine.session() as session:
                stmnt = sa.dialects.postgresql\
                    .insert(StationMARaster)\
                    .values([
                        dict(station_id=self.id,
                             raster_key=self._ma_raster_key,
                             parameter=self._para_base,
                             term=term,
                             value=val,
                             distance=dist)
                        for term, val in zip(
                            self._ma_terms,
                            new_mas)])
                stmnt = stmnt\
                    .on_conflict_do_update(
                        index_elements=["station_id", "raster_key", "parameter", "term"],
                        set_=dict(value=stmnt.excluded.value,
                                  distance=stmnt.excluded.distance)
                    )
                session.execute(stmnt)
                session.commit()

        elif drop_when_error:
            # there was no multi annual data found from the raster
            self._drop(
                why=f"no multi-annual data was found from 'data:rasters:{self._ma_raster_key}'")

    @db_engine.deco_update_privilege
    def update_ma_timeseries(self, kind, **kwargs):
        """Update the mean annual value from the station timeserie.

        Parameters
        ----------
        kind : str or list of str
            The timeseries data kind to update theire multi annual value.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled".
            For the precipitation also "corr" is valid.
        **kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.
        """
        # check kind input
        valid_kinds = self._valid_kinds - {"qn", "filled_by"}
        if kind == "all":
            kind = valid_kinds
        if isinstance(kind, list):
            for kind in self._check_kinds(kind, valids=valid_kinds):
                self.update_ma_timeseries(kind)
            return None
        self._check_kind(kind, valids=valid_kinds)

        # create the sql
        sql = f"""
            WITH ts_y AS (
                SELECT ({self._agg_fun}("{kind}")/count("{kind}")::float*count("timestamp"))::int AS val
                FROM timeseries."{self.id}_{self._para}"
                GROUP BY date_trunc('year', "timestamp")
                HAVING count("{kind}")/count("timestamp")::float>0.9
            )
            INSERT INTO station_ma_timeserie (station_id, parameter, kind, value)
                SELECT *
                FROM (  SELECT
                            {self.id} AS station_id,
                            '{self._para}' AS parameter,
                            '{kind}' AS kind,
                            avg(val)::int AS value
                        FROM ts_y) sq
                WHERE sq.value IS NOT NULL
            ON CONFLICT (station_id, parameter, kind) DO UPDATE
                SET value = EXCLUDED.value;
        """

        # check return_sql
        if kwargs.get("return_sql", False):
            return sql

        # execute the sql
        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    @db_engine.deco_update_privilege
    def _update_last_imp_period_meta(self, period):
        """Update the meta timestamps for a new import."""
        #check period format
        if not isinstance(period, TimestampPeriod):
            period = TimestampPeriod(*period)

        # update meta file
        # ----------------
        # get last_imp valid kinds that are in the meta file
        last_imp_valid_kinds = self._valid_kinds.copy()
        last_imp_valid_kinds.remove("raw")
        for name in {"qn", "filled_by",
                     "raw_min", "raw_max", "filled_min", "filled_max"}:
            if name in last_imp_valid_kinds:
                last_imp_valid_kinds.remove(name)

        # create update sql
        sql_update_meta = '''
            UPDATE meta_{para} meta SET
                raw_from = LEAST (meta.raw_from, {min_tstp}),
                raw_until = GREATEST (meta.raw_until, {max_tstp}),
                last_imp_from = CASE WHEN {last_imp_test}
                                    THEN {min_tstp}
                                    ELSE LEAST(meta.last_imp_from, {min_tstp})
                                    END,
                last_imp_until = CASE WHEN {last_imp_test}
                                    THEN {max_tstp}
                                    ELSE GREATEST(meta.last_imp_until, {max_tstp})
                                    END
                {last_imps}
            WHERE station_id = {stid};
            '''.format(
                para=self._para,
                stid=self.id,
                last_imps=(
                        ", last_imp_" +
                        " = FALSE, last_imp_".join(last_imp_valid_kinds) +
                        " = FALSE"
                    ) if len(last_imp_valid_kinds) > 0 else "",
                last_imp_test=(
                        "meta.last_imp_" +
                        " AND meta.last_imp_".join(last_imp_valid_kinds)
                    ) if len(last_imp_valid_kinds) > 0 else "true",
                **period.get_sql_format_dict(format=f"'{self._tstp_format_db}'"))

        # execute meta update
        with db_engine.connect() as con:
            con.execute(sqltxt(sql_update_meta))
            con.commit()

    @db_engine.deco_update_privilege
    def update_raw(self, only_new=True, ftp_file_list=None, remove_nas=True, **kwargs):
        """Download data from CDC and upload to database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True
        ftp_file_list : list of (strings, datetime), optional
            A list of files on the FTP server together with their modification time.
            If None, then the list is fetched from the server.
            The default is None
        remove_nas : bool, optional
            Remove the NAs from the downloaded data before updating it to the database.
            This has computational advantages.
            The default is True.
        **kwargs : dict
            Additional keyword arguments catch all, but unused here.

        Returns
        -------
        pandas.DataFrame
            The raw Dataframe of the Stations data.
        """
        self._check_min_date(ftp_file_list=ftp_file_list)

        zipfiles = self.get_zipfiles(
            only_new=only_new,
            ftp_file_list=ftp_file_list)

        # check for empty list of zipfiles
        if zipfiles is None or len(zipfiles)==0:
            log.debug(
                f"raw_update of {self._para_long} Station {self.id}:" +
                f"No {'new ' if only_new else ''}zipfile was found and therefor no new data was imported."""
                )
            self._update_last_imp_period_meta(period=(None, None))
            return None

        # download raw data
        df_all, max_hist_tstp_new = self._download_raw(zipfiles=zipfiles.index)

        # cut out valid time period
        min_date = config.get_datetime("weatherdb", "min_date")
        df_all = df_all.loc[df_all.index >= min_date]
        max_hist_tstp_old = self.get_meta(infos=["hist_until"])
        if max_hist_tstp_new is None:
            if max_hist_tstp_old is not None:
                df_all = df_all.loc[df_all.index >= max_hist_tstp_old]
        elif max_hist_tstp_old is None or max_hist_tstp_old <= max_hist_tstp_new:
            self._update_meta(cols=["hist_until"], values=[max_hist_tstp_new])

        # change to db format
        dict_cdc_db = dict(
            zip(self._cdc_col_names_imp, self._db_col_names_imp))
        cols_change = [
            name for name in self._cdc_col_names_imp
            if dict_cdc_db[name] not in self._kinds_not_decimal]
        selection = df_all[self._cdc_col_names_imp].copy()
        selection[cols_change] = (selection[cols_change] * self._decimals)\
            .round(0).astype("Int64")

        # remove NAs in raw column
        raw_col = self._cdc_col_names_imp[
                self._db_col_names_imp.index("raw")]
        selection_without_na = selection[~selection[raw_col].isna()]
        if remove_nas:
            selection = selection_without_na

        # upload to DB
        self._update_db_timeserie(
            selection,
            kinds=self._db_col_names_imp)

        # update raw_files db table
        update_values = \
            ", ".join(
                [f"('{self._para}', '{fp}', '{mod}')" for fp, mod in zip(
                    zipfiles.index,
                    zipfiles["modtime"].dt.strftime("%Y%m%d %H:%M").values)]
            )
        with db_engine.connect() as con:
            con.execute(sqltxt(f'''
                INSERT INTO raw_files(parameter, filepath, modtime)
                VALUES {update_values}
                ON CONFLICT (parameter, filepath) DO UPDATE SET modtime = EXCLUDED.modtime;'''))
            con.commit()

        # if empty skip updating meta filepath
        if len(selection_without_na) == 0:
            log_msg = ("raw_update of {para_long} Station {stid}: " +
                    "The downloaded new dataframe was empty and therefor no new data was imported.")\
                .format(para_long=self._para_long, stid=self.id)
            if not only_new and not self.is_virtual():
                # delete station from meta file because
                # there will never be data for this station
                self._drop(
                    why="while updating raw data with only_new=False the df was empty even thought the station is not virtual")
                log_msg += "\nBecause only_new was False, the station got dropped from the meta file."

            # return empty df
            log.debug(log_msg)
            self._update_last_imp_period_meta(period=(None, None))
            return None
        else:
            self._set_is_real()

        # update multi annual mean
        self.update_ma_timeseries(kind="raw")

        # update meta file
        imp_period = TimestampPeriod(
            selection_without_na.index.min(), selection_without_na.index.max())
        self._update_last_imp_period_meta(period=imp_period)

        log.info(("The raw data for {para_long} station with ID {stid} got "+
             "updated for the period {min_tstp} to {max_tstp}.").format(
                para_long=self._para_long,
                stid=self.id,
                **imp_period.get_sql_format_dict(format=self._tstp_format_human)))

    def get_zipfiles(self, only_new=True, ftp_file_list=None):
        """Get the zipfiles on the CDC server with the raw data.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True
        ftp_file_list : list of (strings, datetime), optional
            A list of files on the FTP server together with their modification time.
            If None, then the list is fetched from the server.
            The default is None

        Returns
        -------
        pandas.DataFrame or None
            A DataFrame of zipfiles and the corresponding modification time on the CDC server to import.
        """
        # check if file list providen
        if ftp_file_list is None:
            ftp_file_list = get_cdc_file_list(
                self._ftp_folders
            )

        # filter for station
        if self._ftp_zip_regex_prefix is not None:
            comp = re.compile(
                self._ftp_zip_regex_prefix + self.id_str + r"[_\.].*")
        else:
            comp = re.compile(r".*_" + self.id_str + r"[_\.].*")
        zipfiles_CDC = list(filter(
            lambda x: comp.match(x[0]),
            ftp_file_list
        ))
        zipfiles_CDC = pd.DataFrame(
            zipfiles_CDC,
            columns=["filepath", "modtime"]
        ).set_index("filepath")

        if only_new:
            # get list of files on CDC Server
            sql_db_modtimes = \
                """SELECT filepath, modtime
                 FROM raw_files
                 WHERE filepath in ({filepaths}) AND parameter='{para}';""".format(
                    filepaths="'" +
                        "', '".join(zipfiles_CDC.index.to_list())
                        + "'",
                    para=self._para)
            with db_engine.connect() as con:
                zipfiles_DB = pd.read_sql(
                    sqltxt(sql_db_modtimes),
                    con=con
                ).set_index("filepath")

            # check for updated files
            zipfiles = zipfiles_CDC.join(
                zipfiles_DB, rsuffix="_DB", lsuffix="_CDC")
            zipfiles = zipfiles[zipfiles["modtime_DB"] != zipfiles["modtime_CDC"]]\
                .drop("modtime_DB", axis=1)\
                .rename({"modtime_CDC": "modtime"}, axis=1)

        else:
            zipfiles = zipfiles_CDC

        # check for empty list of zipfiles
        if len(zipfiles) == 0:
            return None
        else:
            return zipfiles

    def _download_raw(self, zipfiles):
        # download raw data
        # import every file and merge data
        max_hist_tstp = None
        for zf in zipfiles:
            df_new = get_dwd_file(zf)
            df_new.set_index(self._cdc_date_col, inplace=True)
            df_new = self._check_df_raw(df_new)

            # check if hist in query and get max tstp of it ##########
            if "historical" in zf:
                max_hist_tstp_new = df_new.index.max()
                if max_hist_tstp is not None:
                    max_hist_tstp = np.max([max_hist_tstp, max_hist_tstp_new])
                else:
                    max_hist_tstp = max_hist_tstp_new

            # merge with df_all
            if "df_all" not in locals():
                df_all = df_new.copy()
            else:
                # cut out if already in previous file
                df_new = df_new[~df_new.index.isin(df_all.index)]
                # concatenate the dfs
                df_all = pd.concat([df_all, df_new])

        # check for duplicates in date column
        if df_all.index.has_duplicates:
            df_all = df_all.groupby(df_all.index).mean()

        return df_all, max_hist_tstp

    def download_raw(self, only_new=False):
        """Download the timeserie from the CDC Server.

        This function only returns the timeserie, but is not updating the database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is False.

        Returns
        -------
        pandas.DataFrame
            The Timeseries as a DataFrame with a Timestamp Index.
        """
        zipfiles = self.get_zipfiles(only_new=only_new)
        if len(zipfiles)>0:
            return self._download_raw(zipfiles=zipfiles.index)[0]
        else:
            return None

    @db_engine.deco_update_privilege
    def _get_sql_new_qc(self, period=(None, None)):
        """Create the SQL statement for the new quality checked data.

        Needs to have one column timestamp and one column qc.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to do the quality check.

        Returns
        -------
        str
            The sql statement for the new quality controlled timeserie.
        """
        pass  # define in the specific classes

    @db_engine.deco_update_privilege
    def quality_check(self, period=(None, None), **kwargs):
        """Quality check the raw data for a given period.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        **kwargs : dict, optional
            Additional keyword arguments catch all, but unused here.
        """
        period = self._check_period(period=period, kinds=["raw"], nas_allowed=True)

        # create update sql
        sql_qc = """
            WITH new_qc as ({sql_new_qc})
            UPDATE timeseries."{stid}_{para}" ts
            SET "qc" = new."qc"
            FROM new_qc new
            WHERE ts.timestamp = new.timestamp
                AND ts."qc" IS DISTINCT FROM new."qc";
        """.format(
            sql_new_qc=self._get_sql_new_qc(period=period),
            stid=self.id, para=self._para)

        # calculate the percentage of dropped values
        sql_qc += f"""
            UPDATE meta_{self._para}
            SET "qc_dropped" = ts."qc_dropped"
            FROM (
                SELECT ROUND(((count("raw")-count("qc"))::numeric/count("raw")), 4)*100 as qc_dropped
                FROM timeseries."{self.id}_{self._para}"
            ) ts
            WHERE station_id = {self.id};"""

        # run commands
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_qc
        self._execute_long_sql(
            sql=sql_qc,
            description="quality checked for the period {min_tstp} to {max_tstp}.".format(
                **period.get_sql_format_dict(
                    format=self._tstp_format_human)
                ))

        # update multi annual mean
        self.update_ma_timeseries(kind="qc")

        # update timespan in meta table
        self.update_period_meta(kind="qc")

        # mark last import as done if in period
        last_imp_period = self.get_last_imp_period()
        if last_imp_period.inside(period):
            self._mark_last_imp_done(kind="qc")

    @db_engine.deco_update_privilege
    def fillup(self, period=(None, None), **kwargs):
        """Fill up missing data with measurements from nearby stations.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to gap fill the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        **kwargs : dict, optional
            Additional arguments for the fillup function.
            e.g. p_elev to consider the elevation to select nearest stations. (only for T and ET)
        """
        self._expand_timeserie_to_period()
        self._check_ma()

        sql_format_dict = dict(
            stid=self.id, para=self._para, para_base=self._para_base,
            para_escaped=self._para.replace("_", "\\_"),
            ma_terms=", ".join(self._ma_terms),
            coef_sign=self._coef_sign,
            ma_raster_key=self._ma_raster_key,
            base_col="qc" if "qc" in self._valid_kinds else "raw",
            cond_mas_not_null=" OR ".join([
                "ma_other.{ma_col} IS NOT NULL".format(ma_col=ma_col)
                    for ma_col in self._ma_terms]),
            filled_by_col="NULL::smallint AS filled_by",
            exit_cond="SUM((filled IS NULL)::int) = 0",
            extra_unfilled_period_where="",
            add_meta_col="",
            max_fillup_dist=config.get("weatherdb:max_fillup_distance", "p", fallback=200000),
            **self._sql_fillup_extra_dict(**kwargs)
        )

        # make condition for period
        if not isinstance(period, TimestampPeriod):
                period = TimestampPeriod(*period)
        if not period.is_empty():
            sql_format_dict.update(dict(
                cond_period=" WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}".format(
                    **period.get_sql_format_dict(
                        format="'{}'".format(self._tstp_format_db)))
            ))
        else:
            sql_format_dict.update(dict(
                cond_period=""))

        # check if winter/summer or only yearly regionalisation
        if len(self._ma_terms) == 1:
            sql_format_dict.update(dict(
                is_winter_col="",
                coef_calc="ma_stat.{ma_term}{coef_sign[0]}ma_other.{ma_term}::float AS coef"
                .format(
                    ma_term=self._ma_terms[0],
                    coef_sign=self._coef_sign),
                coef_format="i.coef",
                filled_calc="round(nb.{base_col} {coef_sign[1]} %3$s, 0)::int"
                .format(**sql_format_dict)
            ))
        elif len(self._ma_terms) == 2:
            sql_format_dict.update(dict(
                is_winter_col=""",
                    CASE WHEN EXTRACT(MONTH FROM timestamp) IN (1, 2, 3, 10, 11, 12)
                        THEN true::bool
                        ELSE false::bool
                    END AS is_winter""",
                coef_calc=(
                    "ma_stat.{ma_terms[0]}{coef_sign[0]}ma_other.{ma_terms[0]}::float AS coef_wi, \n" + " "*24 +
                    "ma_stat.{ma_terms[1]}{coef_sign[0]}ma_other.{ma_terms[1]}::float AS coef_so"
                ).format(
                    ma_terms=self._ma_terms,
                    coef_sign=self._coef_sign),
                coef_format="i.coef_wi, \n" + " " * 24 + "i.coef_so",
                filled_calc="""
                    CASE WHEN nf.is_winter
                        THEN round(nb.{base_col} {coef_sign[1]} %3$s, 0)::int
                        ELSE round(nb.{base_col} {coef_sign[1]} %4$s, 0)::int
                    END""".format(**sql_format_dict)
            ))
        else:
            raise ValueError(
                "There were too many multi annual columns selected. The fillup method is only implemented for yearly or half yearly regionalisations")

        # raster stats cols
        sql_format_dict["rast_val_cols"] = ", ".join(
            [f"avg(value) FILTER (WHERE \"term\"='{term}') AS \"{term}\""
             for term in self._ma_terms])

        # check if filled_by column is ARRAY or smallint
        if self._filled_by_n>1:
            sql_array_init = "ARRAY[{0}]".format(
                ", ".join(["NULL::smallint"] * self._filled_by_n))

            # create execute sql command
            sql_exec_fillup=""
            prev_check = ""
            for i in range(1, self._filled_by_n+1):
                sql_exec_fillup += f"""
                UPDATE new_filled_{self.id}_{self._para} nf
                SET nb_mean[{i}]=round(nb.qc + %3$s, 0)::int,
                    {sql_format_dict["extra_exec_cols"].format(i=i)}
                    filled_by[{i}]=%1$s
                FROM timeseries.%2$I nb
                WHERE nf.filled IS NULL AND nf.nb_mean[{i}] IS NULL {prev_check}
                    AND nf.timestamp = nb.timestamp;"""
                prev_check += f" AND nf.nb_mean[{i}] IS NOT NULL AND nf.filled_by[{i}] != %1$s"

            sql_format_dict.update(dict(
                filled_by_col = "NULL::smallint[] AS filled_by",
                extra_new_temp_cols = sql_format_dict["extra_new_temp_cols"] +
                    f"{sql_array_init} AS nb_mean,",
                sql_exec_fillup=sql_exec_fillup,
                extra_unfilled_period_where="AND nb_mean[3] is NULL",
                extra_fillup_where=sql_format_dict["extra_fillup_where"] +\
                    ' OR NOT (ts."filled_by" @> new."filled_by" AND ts."filled_by" <@ new."filled_by")'
                ))

            # create exit condition
            sql_format_dict.update(dict(
                exit_cond=f"SUM((filled IS NULL AND nb_mean[{self._filled_by_n}] is NULL)::int) = 0 "))
            if self._fillup_max_dist is not None:
                sql_format_dict.update(dict(
                    add_meta_col=", ST_DISTANCE(geometry_utm,(SELECT geometry_utm FROM stat_row)) as dist",
                    exit_cond=sql_format_dict["exit_cond"]\
                        +f"OR ((i.dist > {self._fillup_max_dist}) AND SUM((filled IS NULL AND nb_mean[1] is NULL)::int) = 0)"
                    ))

            # create sql after loop, to calculate the median of the regionalised neighbors
            sql_format_dict.update(dict(
                sql_extra_after_loop = """UPDATE new_filled_{stid}_{para} SET
                        filled=(SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY v)
                                FROM unnest(nb_mean) as T(v)) {extra_after_loop_extra_col}
                    WHERE filled is NULL;
                    {sql_extra_after_loop}""".format(**sql_format_dict)))
        else:
            # create execute command if only 1 neighbor is considered
            sql_format_dict.update(dict(
                sql_exec_fillup="""
                    UPDATE new_filled_{stid}_{para} nf
                    SET filled={filled_calc}, {extra_cols_fillup_calc}
                        filled_by=%1$s
                    FROM timeseries.%2$I nb
                    WHERE nf.filled IS NULL AND nb.{base_col} IS NOT NULL
                        AND nf.timestamp = nb.timestamp;""".format(**sql_format_dict),
                extra_fillup_where=sql_format_dict["extra_fillup_where"] +\
                    ' OR ts."filled_by" IS DISTINCT FROM new."filled_by"'))

        # Make SQL statement to fill the missing values with values from nearby stations
        sql = """
            CREATE TEMP TABLE new_filled_{stid}_{para}
                ON COMMIT DROP
                AS (SELECT timestamp, {base_col} AS filled,
                        {extra_new_temp_cols}{filled_by_col}{is_winter_col}
                    FROM timeseries."{stid}_{para}" ts {cond_period});
            ALTER TABLE new_filled_{stid}_{para} ADD PRIMARY KEY (timestamp);
            DO
            $do$
                DECLARE i RECORD;
                    unfilled_period RECORD;
                BEGIN
                    SELECT min(timestamp) AS min, max(timestamp) AS max
                    INTO unfilled_period
                    FROM new_filled_{stid}_{para}
                    WHERE "filled" IS NULL;
                    FOR i IN (
                        WITH stat_row AS (
                                SELECT * FROM meta_{para} WHERE station_id={stid}),
                            rast_vals as (
                                SELECT station_id, {rast_val_cols}
                                FROM station_ma_raster
                                WHERE parameter = '{para_base}' and raster_key='{ma_raster_key}'
                                GROUP BY station_id
                            ),
                            meta_dist as (
                                SELECT *, ST_DISTANCE(
                                    geometry_utm,
                                    (SELECT geometry_utm FROM stat_row)) AS dist_m
                                FROM meta_{para})
                        SELECT meta.station_id,
                            meta.raw_from, meta.raw_until,
                            meta.station_id || '_{para}' AS tablename,
                            {coef_calc}{add_meta_col}
                        FROM meta_dist meta
                        LEFT JOIN rast_vals ma_other
                            ON ma_other.station_id=meta.station_id
                        LEFT JOIN (SELECT {ma_terms}
                                   FROM rast_vals
                                   WHERE station_id = {stid}
                                   ) ma_stat
                            ON 1=1
                        WHERE meta.station_id != {stid}
                            AND meta.station_id || '_{para}' IN (
                                SELECT tablename
                                FROM pg_catalog.pg_tables
                                WHERE schemaname ='timeseries'
                                    AND tablename LIKE '%\\_{para_escaped}')
                                AND ({cond_mas_not_null})
                                AND (meta.raw_from IS NOT NULL AND meta.raw_until IS NOT NULL)
                                AND meta.dist_m <= {max_fillup_dist}
                        ORDER BY meta.dist_m {mul_elev_order} ASC)
                    LOOP
                        CONTINUE WHEN i.raw_from > unfilled_period.max
                                      OR i.raw_until < unfilled_period.min
                                      OR (i.raw_from IS NULL AND i.raw_until IS NULL);
                        EXECUTE FORMAT(
                        $$
                        {sql_exec_fillup}
                        $$,
                        i.station_id,
                        i.tablename,
                        {coef_format}
                        );
                        EXIT WHEN (SELECT {exit_cond}
                                   FROM new_filled_{stid}_{para});
                        SELECT min(timestamp) AS min, max(timestamp) AS max
                        INTO unfilled_period
                        FROM new_filled_{stid}_{para}
                        WHERE "filled" IS NULL {extra_unfilled_period_where};
                    END LOOP;
                    {sql_extra_after_loop}
                    UPDATE timeseries."{stid}_{para}" ts
                    SET filled = new.filled, {extra_cols_fillup}
                        filled_by = new.filled_by
                    FROM new_filled_{stid}_{para} new
                    WHERE ts.timestamp = new.timestamp
                        AND (ts."filled" IS DISTINCT FROM new."filled" {extra_fillup_where}) ;
                END
            $do$;
        """.format(**sql_format_dict)

        # execute
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql
        self._execute_long_sql(
            sql=sql,
            description="filled for the period {min_tstp} - {max_tstp}".format(
                **period.get_sql_format_dict(format=self._tstp_format_human)))

        # update multi annual mean
        self.update_ma_timeseries(kind="filled")

        # update timespan in meta table
        self.update_period_meta(kind="filled")

        # mark last imp done
        if (("qc" not in self._valid_kinds) or
                (self.is_last_imp_done(kind="qc"))):
            if period.is_empty():
                self._mark_last_imp_done(kind="filled")
            elif period.contains(self.get_last_imp_period()):
                self._mark_last_imp_done(kind="filled")

    @db_engine.deco_update_privilege
    def _sql_fillup_extra_dict(self, **kwargs):
        """Get the sql statement for the fill to calculate the filling of additional columns.

        This is mainly for the temperature Station to fillup max and min
        and returns an empty string for the other stations.

        And for the precipitation Station and returns an empty string for the other stations.

        Returns
        -------
        dict
            A dictionary with the different additional sql_format_dict entries.
        """
        return {"sql_extra_after_loop": "",
                "extra_new_temp_cols": "",
                "extra_cols_fillup": "",
                "extra_cols_fillup_calc": "",
                "extra_fillup_where": "",
                "mul_elev_order": "",
                "extra_exec_cols": "",
                "extra_after_loop_extra_col": ""}

    @db_engine.deco_update_privilege
    def _mark_last_imp_done(self, kind):
        """Mark the last import for the given kind as done.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        """
        kind = self._check_kind(kind)
        sql = """
            UPDATE meta_{para}
            SET last_imp_{kind} = TRUE
            WHERE station_id = {stid}
        """.format(stid=self.id, para=self._para, kind=kind)

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    @db_engine.deco_update_privilege
    def last_imp_quality_check(self, **kwargs):
        """Do the quality check of the last import.

        Parameters
        ----------
        **kwargs : dict, optional
            Additional keyword arguments passed to the quality_check function.
        """
        if not self.is_last_imp_done(kind="qc"):
            self.quality_check(period=self.get_last_imp_period(), **kwargs)

    @db_engine.deco_update_privilege
    def last_imp_qc(self, **kwargs):
        self.last_imp_quality_check(**kwargs)

    @db_engine.deco_update_privilege
    def last_imp_fillup(self, _last_imp_period=None, **kwargs):
        """Do the gap filling of the last import.

        Parameters
        ----------
        _last_imp_period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to do the gap filling.
            If None is given, the last import period is taken.
            This is only for internal use, to speed up the process if run in a batch.
            The default is None.
        **kwargs : dict, optional
            Additional keyword arguments passed to the fillup function.
        """
        if not self.is_last_imp_done(kind="filled"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.fillup(period=period, **kwargs)

    @classmethod
    def get_meta_explanation(cls, infos="all"):
        """Get the explanations of the available meta fields.

        Parameters
        ----------
        infos : list or string, optional
            The infos you wish to get an explanation for.
            If "all" then all the available information get returned.
            The default is "all"

        Returns
        -------
        pd.Series
            a pandas Series with the information names as index and the explanation as values.
        """
        return pd.Series(
            {c.key: c.comment
             for c in sa.inspect(cls._MetaModel).c
             if infos == "all" or c.key in infos})

    def get_meta(self, infos="all"):
        """Get Information from the meta table.

        Parameters
        ----------
        infos : list of str or str, optional
            A list of the information to get from the database.
            If "all" then all the information are returned.
            The default is "all".

        Returns
        -------
        dict or int/string
            dict with the meta information.
            The first level has one entry per parameter.
            The second level has one entry per information, asked for.
            If only one information is asked for, then it is returned as single value and not as subdict.
        """
        # check which information to get
        if isinstance(infos, str) and (infos == "all"):
            cols = self._MetaModel.__table__.columns
        else:
            if isinstance(infos, str):
                infos = [infos]
            cols = [self._MetaModel.__table__.columns[col]
                    for col in infos]

        # create query
        stmnt = sa.select(*cols).where(self._MetaModel.station_id == self.id)

        with db_engine.session() as con:
            res = con.execute(stmnt)
            keys = res.keys()
            values = res.fetchone()
        if len(keys)==1:
            return values[0]
        else:
            return dict(zip(keys, values))

    def get_geom(self, crs=None):
        """Get the point geometry of the station.

        Parameters
        ----------
        crs: str, int or None, optional
            The coordinate reference system of the geometry.
            If None, then the geometry is returned in WGS84 (EPSG:4326).
            If string, then it should be in a pyproj readable format.
            If int, then it should be the EPSG code.
            The default is None.

        Returns
        -------
        shapely.geometries.Point
            The location of the station as shapely Point in the given coordinate reference system.
        """
        # get the geom
        geom_wkb = self.get_meta(infos=["geometry"])
        geom_shp = shapely.wkb.loads(geom_wkb.data.tobytes())

        # transform
        if crs is not None:
            transformer = pyproj.Transformer.from_proj(
                geom_wkb.srid,
                crs, always_xy=True)
            geom_shp = shapely.ops.transform(
                transformer.transform, geom_shp)

        return geom_shp

    def get_geom_shp(self, crs=None):
        """Get the geometry of the station as a shapely Point object.

        .. deprecated:: 1.0.0
            `get_geom_shp` is deprecated and will be removed in future releases.
            It is replaced by `get_geom`.

        Parameters
        ----------
        crs: str, int or None, optional
            If None, then the geometry is returned in WGS84 (EPSG:4326).
            If string, then it should be one of "WGS84" or "UTM".
            If int, then it should be the EPSG code.

        Returns
        -------
        shapely.geometries.Point
            The location of the station as shapely Point.
        """
        warnings.warn(
            "This function is deprecated and will disapear in future releases. Use get_geom instead.",
            PendingDeprecationWarning)
        return self.get_geom(crs=crs)

    def get_name(self):
        return self.get_meta(infos="stationsname")

    def get_quotient(self, kinds_num, kinds_denom, return_as="df"):
        """Get the quotient of multi-annual means of two different kinds or the timeserie and the multi annual raster value.

        $quotient = \overline{ts}_{kind_num} / \overline{ts}_{denom}$

        Parameters
        ----------
        kinds_num : list of str or str
            The timeseries kinds of the numerators.
            Should be one of ['raw', 'qc', 'filled'].
            For precipitation also "corr" is possible.
        kinds_denom : list of str or str
            The timeseries kinds of the denominator or the multi annual raster key.
            If the denominator is a multi annual raster key, then the result is the quotient of the timeserie and the raster value.
            Possible values are:
                - for timeserie kinds: 'raw', 'qc', 'filled' or for precipitation also "corr".
                - for raster keys: 'hyras', 'dwd' or 'regnie', depending on your defined raster files.
        return_as : str, optional
            The format of the return value.
            If "df" then a pandas DataFrame is returned.
            If "json" then a list with dictionaries is returned.

        Returns
        -------
        pandas.DataFrame or list of dict
            The quotient of the two timeseries as DataFrame or list of dictionaries (JSON) depending on the return_as parameter.
            The default is pd.DataFrame.

        Raises
        ------
        ValueError
            If the input parameters were not correct.
        """
        # check kinds
        rast_keys = {"hyras", "regnie", "dwd"}
        kinds_num = self._check_kinds(kinds_num)
        kinds_denom = self._check_kinds(
            kinds_denom,
            valids=self._valid_kinds | rast_keys)

        # get the quotient from the database views
        with db_engine.session() as con:
            return _get_quotient(
                con=con,
                stids=self.id,
                paras=self._para,
                kinds_num=kinds_num,
                kinds_denom=kinds_denom,
                return_as=return_as)

    def count_holes(self,
            weeks=[2, 4, 8, 12, 16, 20, 24], kind="qc", period=(None, None),
            between_meta_period=True, crop_period=False, **kwargs):
        """Count holes in timeseries depending on there length.

        Parameters
        ----------
        weeks : list, optional
            A list of hole length to count.
            Every hole longer than the duration of weeks specified is counted.
            The default is [2, 4, 8, 12, 16, 20, 24]
        kind : str
            The kind of the timeserie to analyze.
            Should be one of ['raw', 'qc', 'filled'].
            For N also "corr" is possible.
            Normally only "raw" and "qc" make sense, because the other timeseries should not have holes.
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to analyze the timeseries.
            If None is given, the maximum and minimal possible Timestamp is taken.
            The default is (None, None).
        between_meta_period : bool, optional
            Only check between the respective period that is defined in the meta table.
            If "qc" is chosen as kind, then the "raw" meta period is taken.
            The default is True.
        crop_period : bool, optional
            should the period get cropped to the maximum filled period.
            This will result in holes being ignored when they are at the end or at the beginning of the timeserie.
            If period = (None, None) is given, then this parameter is set to True.
            The default is False.

        Returns
        -------
        pandas.DataFrame
            A Pandas Dataframe, with station_id as index and one column per week.
            The numbers in the table are the amount of NA-periods longer than the respective amount of weeks.

        Raises
        ------
        ValueError
            If the input parameters were not correct.
        """
        # check input parameters
        kind = self._check_kind(kind)
        kind_meta_period = "raw" if kind == "qc" else kind

        if period == (None,None):
            crop_period = True
        period = self._check_period(
            period, nas_allowed=not crop_period, kinds=[kind])

        if not isinstance(weeks, list):
            weeks = [weeks]
        if not all([isinstance(el, int) for el in weeks]):
            raise ValueError(
                "Not all the elements of the weeks input parameters where integers.")

        # create SQL statement
        sql_format_dict = dict(
            stid=self.id, para=self._para,
            kind=kind, kind_meta_period=kind_meta_period,
            count_weeks=",".join(
                [f"COUNT(*) FILTER (WHERE td.diff >= '{w} weeks'::INTERVAL) as \"holes>={w} weeks\""
                    for w in weeks]),
            where_between_raw_period="",
            union_from="",
            **period.get_sql_format_dict()
        )
        if between_meta_period:
            sql_format_dict.update(dict(
                where_between_raw_period=\
                f"AND ts.timestamp>=(SELECT {kind_meta_period}_from FROM meta) \
                 AND ts.timestamp<=(SELECT {kind_meta_period}_until FROM meta)",
                union_from=f"UNION (SELECT {kind_meta_period}_from FROM meta)",
                union_until=f"UNION (SELECT {kind_meta_period}_until FROM meta)"
            ))

        sql = """
        WITH meta AS (
            SELECT {kind_meta_period}_from, {kind_meta_period}_until FROM meta_p WHERE station_id={stid})
        SELECT {count_weeks}
        FROM (
            SELECT tst.timestamp-LAG(tst.timestamp) OVER (ORDER BY tst.timestamp) as diff
            FROM (
                SELECT timestamp
                FROM timeseries."{stid}_{para}" ts
                WHERE (ts.timestamp BETWEEN {min_tstp} AND {max_tstp})
                    AND ts.{kind} IS NOT NULL
                    {where_between_raw_period}
                UNION (SELECT {min_tstp} as timestamp {union_from})
                UNION (SELECT {max_tstp} as timestamp {union_until})
                ) tst
            ) td;
        """.format(**sql_format_dict)

        # get response from server
        if "return_sql" in kwargs:
            return sql
        with db_engine.connect() as con:
            res = pd.read_sql(sqltxt(sql), con)

        # set index
        res["station_id"] = self.id
        res.set_index("station_id", inplace=True)

        return res

    def get_period_meta(self, kind, all=False):
        """Get a specific period from the meta information table.

        This functions returns the information from the meta table.
        In this table there are several periods saved, like the period of the last import.

        Parameters
        ----------
        kind : str
            The kind of period to return.
            Should be one of ['filled', 'raw', 'last_imp'].
            filled: the maximum filled period of the filled timeserie.
            raw: the maximum filled timeperiod of the raw data.
            last_imp: the maximum filled timeperiod of the last import.
        all : bool, optional
            Should the maximum Timespan for all the filled periods be returned.
            If False only the period for this station is returned.
            The default is False.

        Returns
        -------
        TimestampPeriod:
            The TimestampPeriod of the station or of all the stations if all=True.

        Raises
        ------
        ValueError
            If a wrong kind is handed in.
        """
        # check kind
        kind = self._check_kind_tstp_meta(kind)

        # create sql statement
        sql_format_dict = dict(para=self._para, stid=self.id, kind=kind)
        if all:
            sql = """
                SELECT min({kind}_from) as {kind}_from,
                    max({kind}_until) as {kind}_until
                FROM meta_{para};
            """.format(**sql_format_dict)
        else:
            sql = """
                SELECT {kind}_from, {kind}_until
                FROM meta_{para}
                WHERE station_id = {stid};
            """.format(**sql_format_dict)

        with db_engine.connect() as con:
            res = con.execute(sa.text(sql))

        return TimestampPeriod(*res.first())

    def get_filled_period(self, kind, from_meta=False):
        """Get the min and max Timestamp for which there is data in the corresponding timeserie.

        Computes the period from the timeserie or meta table.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        from_meta : bool, optional
            Should the period be from the meta table?
            If False: the period is returned from the timeserie. In this case this function is only a wrapper for .get_period_meta.
            The default is False.

        Raises
        ------
        NotImplementedError
            If the given kind is not valid.
        ValueError
            If the given kind is not a string.

        Returns
        -------
        TimestampPeriod
            A TimestampPeriod of the filled timeserie.
            (NaT, NaT) if the timeserie is all empty or not defined.
        """
        if from_meta:
            return self.get_period_meta(kind=kind, all=False)

        kind = self._check_kind(kind=kind)

        if self.isin_db():
            sql = """
                SELECT min(timestamp), max(timestamp)
                FROM timeseries."{stid}_{para}"
                WHERE "{kind}" is not NULL
            """.format(stid=self.id, kind=kind, para=self._para)
            with db_engine.connect() as con:
                respond = con.execute(sqltxt(sql)).first()

            return TimestampPeriod(*respond)
        else:
            return TimestampPeriod(None, None)

    def get_max_period(self, kinds, nas_allowed=False, **kwargs):
        """Get the maximum available period for this stations timeseries.

        If nas_allowed is True, then the maximum range of the timeserie is returned.
        Else the minimal filled period is returned

        Parameters
        ----------
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is False.

        Returns
        -------
        TimestampPeriod
            The maximum Timestamp Period
        """
        if nas_allowed:
            sql_max_tstp = """
                SELECT MIN("timestamp"), MAX("timestamp")
                FROM timeseries."{stid}_{para}";
                """.format(
                    stid=self.id, para=self._para)
            with db_engine.connect() as con:
                res = con.execute(sqltxt(sql_max_tstp))
                max_period = TimestampPeriod(*res.first())
        else:
            kinds = self._check_kinds(kinds)
            if len(kinds)>0:
                max_period = self.get_filled_period(kind=kinds[0], **kwargs)
                for kind in kinds[1:]:
                    max_period = max_period.union(
                        self.get_filled_period(kind=kind, **kwargs),
                        how="outer" if nas_allowed else "inner")
            else:
                max_period = TimestampPeriod(None, None)

        return max_period

    def get_last_imp_period(self, all=False):
        """Get the last imported Period for this Station.

        Parameters
        ----------
        all : bool, optional
            Should the maximum Timespan for all the last imports be returned.
            If False only the period for this station is returned.
            The default is False.

        Returns
        -------
        TimespanPeriod or tuple of datetime.datetime:
                (minimal datetime, maximal datetime)
        """
        return self.get_period_meta(kind="last_imp", all=all)

    def _get_sql_nbs_elev_order(self, p_elev=None):
        """Get the sql part for the elevation order.
        Needs to have stat_row defined. e.g with the following statement:
        WITH stat_row AS (SELECT * FROM meta_{para} WHERE station_id={stid})
        """
        if p_elev is not None:
            if len(p_elev) != 2:
                raise ValueError("p_elev must be a tuple of length 2 or None")
            return f"""*(1+power(
                        abs(stationshoehe - (SELECT stationshoehe FROM stat_row))
                        /{p_elev[0]}::float,
                        {p_elev[1]}::float))"""
        else:
            return ""

    def get_neighboor_stids(self, n=5, only_real=True, p_elev=None, period=None, **kwargs):
        """Get a list with Station Ids of the nearest neighboor stations.

        Parameters
        ----------
        n : int, optional
            The number of stations to return.
            If None, then all the possible stations are returned.
            The default is 5.
        only_real: bool, optional
            Should only real station get considered?
            If false also virtual stations are part of the result.
            The default is True.
        p_elev : tuple of float or None, optional
            The parameters (P_1, P_2) to weight the height differences between stations.
            The elevation difference is considered with the formula from LARSIM (equation 3-18 & 3-19 from the LARSIM manual [1]_ ):

            .. math::

                L_{weighted} = L_{horizontal} * (1 + (\\frac{|\\delta H|}{P_1})^{P_2})
            If None, then the height difference is not considered and only the nearest stations are returned.
            The default is None.
        period : TimestampPeriod or None, optional
            The period for which the nearest neighboors are returned.
            The neighboor station needs to have raw data for at least one half of the period.
            If None, then the availability of the data is not checked.
            The default is None.

        Returns
        -------
        list of int
            A list of station Ids in order of distance.
            The closest station is the first in the list.

        References
        ----------
        .. [1] LARSIM Dokumentation, last check on 06.04.2023, online available under https://www.larsim.info/dokumentation/LARSIM-Dokumentation.pdf
        """
        self._check_isin_meta()

        sql_dict = dict(
            cond_only_real="AND is_real" if only_real else "",
            stid=self.id, para=self._para, n=n,
            add_meta_rows="", cond_period="", mul_elev_order="")

        # Elevation parts
        if p_elev is not None:
            if len(p_elev) != 2:
                raise ValueError("p_elev must be a tuple of length 2 or None")
            sql_dict.update(dict(
                add_meta_rows=", stationshoehe",
                mul_elev_order = self._get_sql_nbs_elev_order(p_elev=p_elev)
                ))

        # period parts
        if period is not None:
            if not isinstance(period, TimestampPeriod):
                period = TimestampPeriod(*period)
            days = period.get_interval().days
            tmstp_mid = period.get_middle()
            sql_dict.update(dict(
                cond_period=f""" AND (raw_until - raw_from > '{np.round(days/2)} days'::INTERVAL
                    AND (raw_from <= '{tmstp_mid.strftime("%Y%m%d")}'::timestamp
                        AND raw_until >= '{tmstp_mid.strftime("%Y%m%d")}'::timestamp)) """
            ))

        # create sql statement
        sql_nearest_stids = """
            WITH stat_row AS (
                SELECT geometry_utm {add_meta_rows}
                FROM meta_{para} WHERE station_id={stid}
            )
            SELECT station_id
            FROM meta_{para}
            WHERE station_id != {stid} {cond_only_real} {cond_period}
            ORDER BY ST_DISTANCE(geometry_utm,(SELECT geometry_utm FROM stat_row))
                {mul_elev_order}
            LIMIT {n};
            """.format(**sql_dict)

        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_nearest_stids

        with db_engine.connect() as con:
            result = con.execute(sqltxt(sql_nearest_stids))
            nearest_stids = [res[0] for res in result.all()]
        return nearest_stids

    def get_multi_annual_raster(self):
        """Get the multi annual raster value(s) for this station.

        Returns
        -------
        list or None
            The corresponding multi annual value.
            For T en ET the yearly value is returned.
            For N the winter and summer half yearly sum is returned in tuple.
            The returned unit is mm or °C.
        """
        sql_select = sa\
            .select(StationMARaster.term,
                    StationMARaster.value)\
            .where(sa.and_(
                StationMARaster.station_id == self.id,
                StationMARaster.raster_key == self._ma_raster_key,
                StationMARaster.parameter == self._para_base,
                StationMARaster.term.in_(self._ma_terms)))
        with db_engine.session() as session:
            res = session.execute(sql_select).all()

        # Update ma values if no result returned
        if res is None:
            self.update_ma_raster()
            with db_engine.session() as session:
                res = session.execute(sql_select).all()

        if len(res) == 0:
            return None
        else:
            res_dict = dict(res)
            return [float(np.round(res_dict[term] / self._decimals, self._decimals//10))
                    for term in self._ma_terms]

    def get_ma_raster(self):
        """Wrapper for `get_multi_annual`."""
        return self.get_multi_annual_raster()

    def _get_raster_value(self, raster_conf, bands="all", dist=0):
        """Get the value of a raster file for this station.

        Parameters
        ----------
        raster_conf : dict or configparser.SectionProxy
            The configuration of the raster file.
        bands : str or int or list of str or int, optional
            The band to get the value from.
            If "all" then all bands are returned.
            If int, then the band with the respective number is returned.
            If str, then the band is first checked to be a key in the raster_conf and then this band is returned.
            If no key in raster_conf, then name is checked against the raster names and this band is returned.
            The default is "all".
        dist : int, optional
            The distance to the station in the rasters CRS.
            Only works for rasters with projected CRS.
            The default is 0.

        Returns
        -------
        numpy.array of int or float or np.nan
            The rasters value at the stations position

        Raises
        ------
        ValueError
            If the raster is not in a projected coordinate system and dist > 0.
        """
        file = Path(raster_conf["file"])
        with rio.open(file) as src:
            # get the CRS
            src_srid = src.crs.to_epsg()
            if src_srid is None:
                src_srid = raster_conf["srid"]

            # get the station geom
            stat_geom = self.get_geom(crs=src_srid)

            # get the bands indexes
            if isinstance(bands, str) and bands.lower() == "all":
                indexes = src.indexes
            else:
                if not isinstance(bands, list):
                    bands = [bands]
                indexes = []
                for band in bands:
                    if isinstance(bands, int) & (band in src.indexes):
                        indexes.append(band)
                    else:
                        if band in raster_conf:
                            band = raster_conf[band]
                        if band in src.descriptions:
                            indexes.append(src.descriptions.index(band)+1)
                        else:
                            raise ValueError(
                                f"The band {band} is not in the raster file {file}.")

            # get the value
            if dist==0:
                return list(
                        src.sample(stat_geom.coords, indexes=indexes, masked=True)
                    )[0].astype(np.float32).filled(np.nan)
            else:
                proj_srid = pyproj.CRS.from_epsg(
                    config.get("weatherdb", "RASTER_BUFFER_CRS"))
                if not proj_srid.is_projected:
                    raise ValueError(textwrap.dedent(
                        """Buffering the stations position to get raster values for nearby raster cells is only allowed for projected rasters.
                        Please update the RASTER_BUFFER_CRS in the weatherdb section of the config file to a projected CRS."""))
                tr_to = pyproj.Transformer.from_proj(src_srid, proj_srid, always_xy=True)
                tr_back = pyproj.Transformer.from_proj(proj_srid, src_srid, always_xy=True)
                buf_geom = shapely.ops.transform(
                    tr_back.transform,
                    shapely.ops.transform(tr_to.transform, stat_geom).buffer(dist),
                )

                return np.array([zonal_stats(
                            buf_geom,
                            file,
                            band_num=band_num,
                            stats=["mean"],
                            all_touched=True
                        )[0]["mean"]
                    for band_num in indexes], dtype=np.float32)

    def get_coef(self, other_stid, in_db_unit=False):
        """Get the regionalisation coefficients due to the height.

        Those are the values from the dwd grid, HYRAS or REGNIE grids.

        Parameters
        ----------
        other_stid : int
            The Station Id of the other station from wich to regionalise for own station.
        in_db_unit : bool, optional
            Should the coefficients be returned in the unit as stored in the database?
            This is only relevant for the temperature.
            The default is False.

        Returns
        -------
        list of floats or None
            A list of coefficients.
            For T, ET and N-daily only the the yearly coefficient is returned.
            For N the winter and summer half yearly coefficient is returned in tuple.
            None is returned if either the own or other stations multi-annual value is not available.
        """
        ma_values = self.get_multi_annual_raster()
        other_stat = self.__class__(other_stid)
        other_ma_values = other_stat.get_multi_annual_raster()

        if other_ma_values is None or ma_values is None:
            return None
        else:
            if self._coef_sign[0] == "/":
                return [own/other for own, other in zip(ma_values, other_ma_values)]
            elif self._coef_sign[0] == "-":
                if in_db_unit:
                    return [int(np.round((own-other)*self._decimals))
                            for own, other in zip(ma_values, other_ma_values)]
                else:
                    return [own-other for own, other in zip(ma_values, other_ma_values)]
            else:
                return None

    def get_df(self, kinds, period=(None, None), agg_to=None,
               nas_allowed=True, add_na_share=False, db_unit=False,
               sql_add_where=None, **kwargs):
        """Get a timeseries DataFrame from the database.

        Parameters
        ----------
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by", "filled_share".
            For the precipitation also "qn" and "corr" are valid.
            If "filled_by" is given together with an aggregation step, the "filled_by" is replaced by the "filled_share".
            The "filled_share" gives the share of filled values in the aggregation group in percent.
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        agg_to : str or None, optional
            Aggregate to a given timespan.
            If more than 20% of missing values in the aggregation group, the aggregated value will be None.
            Can be anything smaller than the maximum timespan of the saved data.
            If a Timeperiod smaller than the saved data is given, than the maximum possible timeperiod is returned.
            For T and ET it can be "month", "year".
            For N it can also be "hour".
            If None than the maximum timeperiod is taken.
            The default is None.
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is True.
        add_na_share : bool, optional
            Should one or several columns be added to the Dataframe with the share of NAs in the data.
            This is especially important, when the stations data get aggregated, because the aggregation doesn't make sense if there are a lot of NAs in the original data.
            If True, one column per asked kind is added with the respective share of NAs, if the aggregation step is not the smallest.
            The "kind"_na_share column is in percentage.
            The default is False.
        db_unit : bool, optional
            Should the result be in the Database unit.
            If False the unit is getting converted to normal unit, like mm or °C.
            The numbers are saved as integer in the database and got therefor multiplied by 10 or 100 to get to an integer.
            The default is False.
        sql_add_where : str or None, optional
            additional sql where statement to filter the output.
            E.g. "EXTRACT(MONTH FROM timestamp) == 2"
            The default is None

        Returns
        -------
        pandas.DataFrame
            The timeserie Dataframe with a DatetimeIndex.
        """
        # check if existing
        if not self.isin_db():
            return None

        # check if adj
        if "adj" in kinds:
            adj_df = self.get_adj(
                period=period, agg_to=agg_to,
                nas_allowed=nas_allowed, add_na_share=add_na_share)
            if len(kinds) == 1:
                return adj_df
            else:
                kinds.remove("adj")

        # check kinds and period
        if "filled_share" in kinds:
            add_filled_share = True
            kinds.remove("filled_share")
        else:
            add_filled_share = False
        kinds = self._check_kinds(kinds=kinds)
        if not ("_skip_period_check" in kwargs and kwargs["_skip_period_check"]):
            period = self._check_period(
                period=period, kinds=kinds, nas_allowed=nas_allowed)

        if period.is_empty() and not nas_allowed:
            return None

        # aggregating?
        timestamp_col = "timestamp"
        group_by = ""
        agg_to = self._check_agg_to(agg_to)
        if agg_to is not None:
            if "filled_by" in kinds:
                warnings.warn(
                    f"""You selected a filled_by column, but did not select the smallest aggregation (agg_to={self._min_agg_to}).
                    The filled_by information is only reasonable when using the original time frequency.
                    Therefor the filled_by column is not returned, but instead the filled_share.
                    This column gives the percentage of the filled fields in the aggregation group.""")
                kinds.remove("filled_by")
                add_filled_share = True

            # create sql parts
            kinds_before = kinds.copy()
            kinds = []
            for kind in kinds_before:
                if re.search(r".*(_min)|(_max)", kind):
                    agg_fun = "MIN" if re.search(r".*_min", kind) else "MAX"
                else:
                    agg_fun = self._agg_fun
                kinds.append(
                    f"CASE WHEN (COUNT(\"{kind}\")/COUNT(*)::float)>0.8 "+
                    f"THEN ROUND({agg_fun}({kind}), 0) ELSE NULL END AS {kind}")

            timestamp_col = "date_trunc('{agg_to}', timestamp)".format(
                agg_to=agg_to)
            group_by = "GROUP BY " + timestamp_col
            if agg_to in ["day", "month", "year", "decade"]:
                timestamp_col += "::date"

            # add the filled_share if needed
            if add_filled_share:
                kinds.append(
                    'COUNT("filled_by")::float/COUNT(*)::float*100 as filled_share')

            # raise warning, when NA_share should get added
            if any([kind in ["raw", "qc"] for kind in kinds] ) and not add_na_share:
                warnings.warn(
                "You aggregate a column that can contain NAs (e.g. \"raw\" or \"qc\")\n" +
                "This can result in strange values, because in one aggregation group can be many NAs.\n"+
                "To suppress this warning and to consider this effect please use add_na_share=True in the parameters.")

            # create na_share columns
            if add_na_share:
                for kind in kinds_before:
                    kinds.append(f"(COUNT(*)-COUNT(\"{kind}\"))/COUNT(*)::float * 100 AS {kind}_na_share")

        # sql_add_where
        if sql_add_where:
            if "and" not in sql_add_where.lower():
                sql_add_where = " AND " + sql_add_where
        else:
            sql_add_where = ""

        # create base sql
        sql = """
            SELECT {timestamp_col} as timestamp, {kinds}
            FROM timeseries."{stid}_{para}"
            WHERE timestamp BETWEEN {min_tstp} AND {max_tstp}{sql_add_where}
            {group_by}
            ORDER BY timestamp ASC;
            """.format(
                stid=self.id,
                para=self._para,
                kinds=', '.join(kinds),
                group_by=group_by,
                timestamp_col=timestamp_col,
                sql_add_where=sql_add_where,
                **period.get_sql_format_dict(
                    format="'{}'".format(self._tstp_format_db))
            )

        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql

        with db_engine.connect() as con:
            df = pd.read_sql(
                sqltxt(sql),
                con=con,
                index_col="timestamp")

        # convert filled_by to Int16, pandas Integer with NA support
        if "filled_by" in kinds and df["filled_by"].dtype != object:
            df["filled_by"] = df["filled_by"].astype("Int16")

        # change index to pandas DatetimeIndex if necessary
        if not isinstance(df.index, pd.DatetimeIndex):
            df.set_index(pd.DatetimeIndex(df.index), inplace=True)

        # set Timezone to UTC
        df.index = df.index.tz_localize("UTC")

        # change to normal unit
        if not db_unit:
            change_cols = [
                col for col in df.columns
                if col not in self._kinds_not_decimal and "_na_share" not in col]
            df[change_cols] = df[change_cols] / self._decimals

        # check if adj should be added:
        if "adj_df" in locals():
            df = df.join(adj_df)

        return df

    def get_raw(self, **kwargs):
        """Get the raw timeserie.

        Parameters
        ----------
        **kwargs : dict, optional
            The keyword arguments get passed to the get_df function.
            Possible parameters are "period", "agg_to" or "nas_allowed"

        Returns
        -------
        pd.DataFrame
            The raw timeserie for this station and the given period.
        """
        return self.get_df(kinds="raw",**kwargs)

    def get_qc(self, **kwargs):
        """Get the quality checked timeserie.

        Parameters
        ----------
        **kwargs : dict, optional
            The keyword arguments get passed to the get_df function.
            Possible parameters are "period", "agg_to" or "nas_allowed"

        Returns
        -------
        pd.DataFrame
            The quality checked timeserie for this station and the given period.
        """
        return self.get_df(kinds="qc", **kwargs)

    def get_dist(self, period=(None, None)):
        """Get the timeserie with the infomation from which station the data got filled and the corresponding distance to this station.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeserie.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).

        Returns
        -------
        pd.DataFrame
            The timeserie for this station and the given period with the station_id and the distance in meters from which the data got filled from.
        """
        period = self._check_period(period, kinds=["filled"])

        sql = """
            WITH dist AS (
                SELECT
                    station_id,
                    round(ST_DISTANCE(
                        geometry_utm,
                        (SELECT geometry_utm FROM meta_{para}
                         WHERE station_id = {stid})
                    )) AS distance
                FROM meta_{para}
                WHERE station_id!={stid}
            )
            SELECT timestamp, filled_by, distance
            FROM timeseries."{stid}_{para}"
            LEFT JOIN dist ON filled_by=station_id
            WHERE BETWEEN {min_tstp} AND {max_tstp};""".format(
            stid=self.id,
            para=self._para,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format_db)
            )
        )

        with db_engine.connect() as con:
            df = pd.read_sql(
                sqltxt(sql),
                con=con,
                index_col="timestamp")

        # change index to pandas DatetimeIndex if necessary
        if not isinstance(df.index, pd.DatetimeIndex):
            df.set_index(pd.DatetimeIndex(df.index), inplace=True)

        return df

    def get_filled(self, period=(None, None), with_dist=False, **kwargs):
        """Get the filled timeserie.

        Either only the timeserie is returned or also the id of the station from which the station data got filled, together with the distance to this station in m.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeserie.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        with_dist : bool, optional
            Should the distance to the stations from which the timeseries got filled be added.
            The default is False.

        Returns
        -------
        pd.DataFrame
            The filled timeserie for this station and the given period.
        """
        df = self.get_df(period=period, kinds="filled", **kwargs)

        # should the distance information get added
        if with_dist:
            df = df.join(self.get_dist(period=period))

        return df

    def get_adj(self, **kwargs):
        """Get the adjusted timeserie.

        The timeserie is adjusted to the multi annual mean.
        So the overall mean of the given period will be the same as the multi annual mean.

        Parameters
        ----------
        **kwargs : dict, optional
            The keyword arguments are passed to the get_df function.
            Possible parameters are "period", "agg_to" or "nas_allowed".

        Returns
        -------
        pandas.DataFrame
            A timeserie with the adjusted data.
        """
        # this is only the first part of the method
        # get basic values
        main_df = self.get_df(
            kinds=["filled"], # not best, as the ma values are not richter corrected
            **kwargs)
        ma = self.get_multi_annual_raster()

        # create empty adj_df
        adj_df = pd.DataFrame(
            columns=["adj"],
            index=main_df.index,
            dtype=main_df["filled"].dtype)

        return main_df, adj_df, ma  # the rest must get implemented in the subclasses

    def plot(self, period=(None, None), kind="filled", agg_to=None, **kwargs):
        """Plot the data of this station.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kind : str, optional
            The data kind to plot.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
            The default is "filled.
        agg_to : str or None, optional
            Aggregate to a given timespan.
            Can be anything smaller than the maximum timespan of the saved data.
            If a Timeperiod smaller than the saved data is given, than the maximum possible timeperiod is returned.
            For T and ET it can be "month", "year".
            For N it can also be "hour".
            If None than the maximum timeperiod is taken.
            The default is None.
        """
        kinds = []
        if "kinds" in kwargs:
            for kind in kwargs["kinds"]:
                if kind not in kinds:
                    kinds.append(kind)
            kwargs.pop("kinds")
        else:
            kinds = [kind]

        df = self.get_df(kinds=kinds, period=period, db_unit=False, agg_to=agg_to)

        df.plot(
            xlabel="Datum", ylabel=self._unit,
            title="{para_long} Station {stid}".format(
                para_long=self._para_long,
                stid=self.id),
            **kwargs
        )


class StationCanVirtualBase(StationBase):
    """A class to add the methods for stations that can also be virtual.
    Virtual means, that there is no real DWD station with measurements.
    But to have data for every parameter at every 10 min precipitation station location, it is necessary to add stations and fill the gaps with data from neighboors."""

    def _check_isin_meta(self):
        """Check if the Station is in the Meta table and if not create a virtual station.

        Raises:
            NotImplementedError:
                If the Station ID is neither a real station or in the precipitation meta table.

        Returns:
            bool: True if the Station check was successfull.
        """
        if self.isin_meta():
            if self.isin_db():
                return True
            else:
                self._create_timeseries_table()
                return True
        elif self.isin_meta_p():
            self._create_meta_virtual()
            self._create_timeseries_table()
            return True
        raise NotImplementedError(f"""
The given {self._para_long} station with id {self.id} is not in the corresponding meta table
and not in the precipitation meta table in the DB""")

    def _create_meta_virtual(self):
        """Create a virtual station in the meta table, for stations that have no real data.

        Is only working if a corresponding station is in the precipitation stations meta table.
        """
        sql = """
            INSERT INTO meta_{para}(
                station_id, is_real, geometry, geometry_utm,
                stationshoehe, stationsname, bundesland)
            (SELECT station_id, false, geometry, geometry_utm,
                    stationshoehe, stationsname, bundesland
             FROM meta_p
             WHERE station_id = {stid})
        """.format(stid=self.id, para=self._para)

        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    def isin_meta_p(self):
        """Check if Station is in the precipitation meta table.

        Returns
        -------
        bool
            True if Station is in the precipitation meta table.
        """
        with db_engine.connect() as con:
            result = con.execute(sqltxt(
                f"SELECT {self.id} in (SELECT station_id FROM meta_p);"))
        return result.first()[0]

    def quality_check(self, period=(None, None), **kwargs):
        if not self.is_virtual():
            return super().quality_check(period=period, **kwargs)


class StationTETBase(StationCanVirtualBase):
    """A base class for T and ET.

    This class adds methods that are only used by temperatur and evapotranspiration stations.
    """
    # timestamp configurations
    _tstp_format_db = "%Y%m%d"
    _tstp_format_human = "%Y-%m-%d"
    _tstp_dtype = "date"
    _interval = "1 day"

    # aggregation
    _min_agg_to = "day"

    def get_neighboor_stids(self, p_elev=(250, 1.5), **kwargs):
        """Get the 5 nearest stations to this station.

        Parameters
        ----------
        p_elev : tuple, optional
            In Larsim those parameters are defined as $P_1 = 500$ and $P_2 = 1$.
            Stoelzle et al. (2016) found that $P_1 = 100$ and $P_2 = 4$ is better for Baden-Würtemberg to consider the quick changes in topographie.
            For all of germany, those parameter values are giving too much weight to the elevation difference, which can result in getting neighboor stations from the border of the Tschec Republic for the Feldberg station. Therefor the values $P_1 = 250$ and $P_2 = 1.5$ are used as default values.
            literature:
                - Stoelzle, Michael & Weiler, Markus & Steinbrich, Andreas. (2016) Starkregengefährdung in Baden-Württemberg – von der Methodenentwicklung zur Starkregenkartierung. Tag der Hydrologie.
                - LARSIM Dokumentation, Stand 06.04.2023, online unter https://www.larsim.info/dokumentation/LARSIM-Dokumentation.pdf
            The default is (250, 1.5).

        Returns
        -------
        _type_
            _description_
        """
        # define the P1 and P2 default values for T and ET
        return super().get_neighboor_stids(p_elev=p_elev, **kwargs)

    def _get_sql_near_median(self, period, only_real=True,
                             extra_cols=None, add_is_winter=False):
        """Get the SQL statement for the mean of the 5 nearest stations.

        Needs to have one column timestamp, mean and raw(original raw value).

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the mean of the nearest stations.
        only_real: bool, optional
            Should only real station get considered?
            If false also virtual stations are part of the result.
            The default is True.
        extra_cols : str or None, optional
            Should there bae additional columns in the result?
            Should be a sql-string for the SELECT part.
            If None then there are no additional columns.
            The default is None.
        add_is_winter : bool, optional
            Should there be a column ("winter") that indicates if the value is in winter?
            The default is False.

        Returns
        -------
        str
            SQL statement for the regionalised mean of the 5 nearest stations.
        """
        # get neighboring station for every year
        start_year = period.start.year
        end_year = period.end.year
        nbs = pd.DataFrame(
            index=pd.Index(range(start_year, end_year+1), name="years"),
            columns=["near_stids"], dtype=object)
        nbs_stids_all = set()
        now = pd.Timestamp.now()
        for year in nbs.index:
            if year == now.year:
                y_period = TimestampPeriod(f"{year}-01-01", now.date())
            else:
                y_period = TimestampPeriod(f"{year}-01-01", f"{year}-12-31")
            nbs_i = self.get_neighboor_stids(period=y_period, only_real=only_real)
            nbs_stids_all = nbs_stids_all.union(nbs_i)
            nbs.loc[year, "near_stids"] = nbs_i

        # add a grouping column if stids of year before is the same
        before = None
        group_i = 1
        for year, row in nbs.iterrows():
            if before is None:
                before = row["near_stids"]
            if before != row["near_stids"]:
                group_i += 1
                before = row["near_stids"]
            nbs.loc[year, "group"] = group_i


        # aggregate if neighboors are the same
        nbs["start"] = nbs.index
        nbs["end"] = nbs.index
        nbs = nbs.groupby(nbs["group"])\
            .agg({"near_stids":"first", "start": "min", "end": "max"})\
            .set_index(["start", "end"])

        # get coefs for regionalisation from neighbor stations
        coefs = pd.Series(
            index=nbs_stids_all,
            data=[self.get_coef(other_stid=near_stid, in_db_unit=True)
                  for near_stid in nbs_stids_all]
            ).fillna("NULL")\
            .apply(lambda x: x[0] if isinstance(x, list) else x)\
            .astype(str)

        # check extra cols to be in the right format
        if extra_cols and len(extra_cols) > 0:
            if extra_cols[0] != ",":
                extra_cols = ", " + extra_cols
        else:
            extra_cols = ""

        # create sql for winter
        if add_is_winter:
            sql_is_winter_col = ", EXTRACT(MONTH FROM ts.timestamp) in (1,2,3,10,11,12) AS winter"
        else:
            sql_is_winter_col = ""

        # create year subqueries for near stations mean
        sql_near_median_parts = []
        for (start, end), row in nbs.iterrows():
            period_part = TimestampPeriod(f"{start}-01-01", f"{end}-12-31")
            near_stids = row["near_stids"]

            # create sql for mean of the near stations and the raw value itself
            sql_near_median_parts.append("""
                SELECT timestamp,
                    (SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY T.c)
                        FROM (VALUES {reg_vals}) T (c)
                    ) as nbs_median
                FROM timeseries."{near_stids[0]}_{para}" ts1
                {near_joins}
                WHERE timestamp BETWEEN {min_tstp}::{tstp_dtype} AND {max_tstp}::{tstp_dtype}
                """.format(
                    para=self._para,
                    near_stids=near_stids,
                    reg_vals=", ".join(
                        [f"(ts{i+1}.raw{self._coef_sign[1]}{coef})"
                            for i, coef in enumerate(coefs[near_stids].values)]),
                    near_joins = "\n".join(
                        [f"FULL OUTER JOIN timeseries.\"{near_stid}_{self._para}\" ts{i+1} USING (timestamp)"
                            for i, near_stid in enumerate(near_stids)
                            if i>0]),
                    tstp_dtype=self._tstp_dtype,
                    **period_part.get_sql_format_dict()))

        # create sql for mean of the near stations and the raw value itself for total period
        sql_near_median = """SELECT ts.timestamp, nbs_median, ts.raw as raw {extra_cols}{is_winter_col}
            FROM timeseries."{stid}_{para}" AS ts
            LEFT JOIN ({sql_near_parts}) nbs
                ON ts.timestamp=nbs.timestamp
            WHERE ts.timestamp BETWEEN {min_tstp}::{tstp_dtype} AND {max_tstp}::{tstp_dtype}
            ORDER BY timestamp ASC"""\
                .format(
                    stid = self.id,
                    para = self._para,
                    sql_near_parts = " UNION ".join(sql_near_median_parts),
                    tstp_dtype=self._tstp_dtype,
                    extra_cols=extra_cols,
                    is_winter_col=sql_is_winter_col,
                    **period.get_sql_format_dict())

        return sql_near_median

    def _get_sql_nbs_elev_order(self, p_elev=(250, 1.5)):
        """Set the default P values. See _get_sql_near_median for more informations."""
        return super()._get_sql_nbs_elev_order(p_elev=p_elev)

    def fillup(self, p_elev=(250, 1.5), **kwargs):
        """Set the default P values. See _get_sql_near_median for more informations."""
        return super().fillup(p_elev=p_elev, **kwargs)

    def _sql_fillup_extra_dict(self, **kwargs):
        sql_extra_dict = super()._sql_fillup_extra_dict(**kwargs)
        if "p_elev" in kwargs:
            sql_extra_dict.update(dict(
                mul_elev_order=self._get_sql_nbs_elev_order(p_elev=kwargs["p_elev"])))
        else:
            sql_extra_dict.update(dict(
                mul_elev_order=self._get_sql_nbs_elev_order()))
        return sql_extra_dict

    def get_adj(self, **kwargs):
        """Get the adjusted timeserie.

        The timeserie get adjusted to match the multi-annual value over the given period.
        So the yearly variability is kept and only the whole period is adjusted.

        Returns
        -------
        pd.DataFrame
            The adjusted timeserie with the timestamp as index.
        """
        # this is only the second part of the method
        main_df, adj_df, ma = super().get_adj(**kwargs)

        # truncate to full years
        tstp_min = main_df.index.min()
        if tstp_min > pd.Timestamp(year=tstp_min.year, month=1, day=15, tz="UTC"):
            tstp_min = pd.Timestamp(
                year=tstp_min.year+1, month=1, day=1,  tz="UTC")

        tstp_max = main_df.index.max()
        if tstp_max < pd.Timestamp(year=tstp_min.year, month=12, day=15, tz="UTC"):
            tstp_min = pd.Timestamp(
                year=tstp_min.year-1, month=12, day=31, tz="UTC")

        main_df_tr = main_df.truncate(tstp_min, tstp_max)

        # the rest must get implemented in the subclasses
        return main_df, adj_df, ma, main_df_tr


class StationPBase(StationBase):
    # common settings
    _decimals = 100

    # cdc dwd parameters
    _cdc_date_col = "MESS_DATUM"

    # for regionalistaion
    _ma_terms = ["wihy", "suhy"]
    _ma_raster_key = "hyras"

    def get_adj(self, **kwargs):
        """Get the adjusted timeserie.

        The timeserie get adjusted to match the multi-annual value over the given period.
        So the yearly variability is kept and only the whole period is adjusted.

        The basis for the adjusted timeseries is the filled data and not the richter corrected data,
        as the ma values are also uncorrected vallues.

        Returns
        -------
        pd.DataFrame
            The adjusted timeserie with the timestamp as index.
        """
        main_df, adj_df, ma = super().get_adj(**kwargs)

        # calculate the half yearly mean
        # suhy
        suhy_months = [4, 5, 6, 7, 8, 9]
        mask_suhy = main_df.index.month.isin(suhy_months)

        main_df_suhy = main_df[mask_suhy]

        # get the minimum count of elements in the half year
        min_count = (365//2 - 10) # days
        if "agg_to" not in kwargs:
            if self._interval == "10 min":
                min_count = min_count * 24 * 6 # 10 minutes
        else:
            if kwargs["agg_to"] == "month":
                min_count=6
            elif kwargs["agg_to"] == "hour":
                min_count = min_count * 24
            elif kwargs["agg_to"] == "year" or kwargs["agg_to"] == "decade":
                raise ValueError("The get_adj method does not work on decade values.")

        main_df_suhy_y = main_df_suhy.groupby(main_df_suhy.index.year)\
            .sum(min_count=min_count).mean()

        adj_df[mask_suhy] = (main_df_suhy * (ma[1] / main_df_suhy_y)).round(2)

        # wihy
        mask_wihy = ~mask_suhy
        main_df_wihy = main_df[mask_wihy]
        main_df_wihy_y = main_df_wihy.groupby(main_df_wihy.index.year)\
            .sum(min_count=min_count).mean()
        adj_df[mask_wihy] = (main_df_wihy * (ma[0] / main_df_wihy_y)).round(2)

        return adj_df
