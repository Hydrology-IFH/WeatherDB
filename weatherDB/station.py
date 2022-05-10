"""
This module has a class for every type of station. E.g. StationN (or StationN).
One object represents one Station with one parameter.
This object can get used to get the corresponding timeserie.
There is also a StationGroup class that groups the three parameters precipitation, temperature and evapotranspiration together for one station.
"""
# libraries
import itertools
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
import warnings
import zipfile

import numpy as np
import pandas as pd
from sqlalchemy.exc import OperationalError

import rasterio as rio
import rasterio.mask
from shapely.geometry import Point, MultiLineString
import shapely.wkt

from .lib.connections import CDC, DB_ENG, check_superuser
from .lib.max_fun.import_DWD import dwd_id_to_str, get_dwd_file
from .lib.utils import TimestampPeriod, get_ftp_file_list
from .lib.max_fun.geometry import polar_line, raster2points

# Variables
MIN_TSTP = datetime.strptime("19940101", "%Y%m%d").replace(tzinfo=timezone.utc)
# all timestamps in the database are in UTC
THIS_DIR = Path(__file__).parent.resolve()
DATA_DIR = THIS_DIR.parents[2].joinpath("data")
RASTERS = {
    "dwd_grid": {
        "srid": 31467,
        "proj4":"+proj=tmerc +lat_0=0 +lon_0=9 +k=1 +x_0=3500000 +y_0=0 +ellps=bessel +towgs84=598.1,73.7,418.2,0.202,0.045,-2.455,6.7 +units=m +no_defs",
        "db_table": "dwd_grid_1991_2020",
        "bands": {
            1: "n_wihj",
            2: "n_sohj",
            3: "n_year",
            4: "t_year",  # is in 0.1°C
            5: "et_year"
        },
        "dtype": int
    },
    "regnie_grid": {
        "srid": 4326,
        "db_table": "regnie_grid_1991_2020",
        "bands": {
            1: "n_regnie_wihj",
            2: "n_regnie_sohj",
            3: "n_regnie_year"
        },
        "dtype": int
    },
    "local":{
        "dgm5": DATA_DIR.joinpath("dgms/dgm5.tif"),
        "dgm80": DATA_DIR.joinpath("dgms/dgm80.tif")
    }
}
RICHTER_CLASSES = {
    "no-protection": {
        "min_horizon": 0,
        "max_horizon": 3
    },
    "little-protection": {
        "min_horizon": 3,
        "max_horizon": 7
    },
    "protected": {
        "min_horizon": 7,
        "max_horizon": 12
    },
    "heavy-protection": {
        "min_horizon": 12,
        "max_horizon": np.inf
    },
}
AGG_TO = { # possible aggregation periods from small to big
    None: {
        "split":{"n": 5, "t":3, "et": 3}},
    "10 min": {
        "split":{"n": 5, "t":3, "et": 3}},
    "hour": {
        "split":{"n": 4, "t":3, "et": 3}},
    "day": {
        "split":{"n": 3, "t":3, "et": 3}},
    "month": {
        "split":{"n": 2, "t":2, "et": 2}},
    "year": {
        "split":{"n": 1, "t":1, "et": 1}},
    "decade": {
        "split":{"n": 1, "t":1, "et": 1}}
    }

# get log
log = logging.getLogger(__name__)


# class definitions
###################

class StationBase:
    """This is the Base class for one Station.
    It is not working on it's own, because those parameters need to get defined in the real classes
    """
    # because those parameters need to get defined in the real classes:
    _ftp_folder_base = ["None"]  # the base folder on the CDC-FTP server
    _date_col = None  # The name of the date column on the CDC server
    _para = None  # The parameter string "n", "t", or "et"
    _para_long = None  # The parameter as a long descriptive string
    # the name of the CDC column that has the raw data and gets multiplied by the decimals
    _cdc_col_name_raw = None
    # the names of the CDC columns that get imported
    _cdc_col_names_imp = [None]
    # the corresponding column name in the DB of the raw import
    _db_col_names_imp = ["raw"]
    # the kinds that should not get multiplied with the amount of decimals, e.g. "qn"
    _kinds_not_decimal = ["qn", "filled_by", "filled_share"]
    _tstp_format_db = None  # The format string for the strftime for the database to be readable
    _tstp_format_human = "%Y-%m-%d %H:%M" # the format of the timestamp to be human readable
    _unit = "None"  # The Unit as str
    _decimals = 1  # the factor to change data to integers for the database
    # The valid kinds to use. Must be a column in the timeseries tables.
    _valid_kinds = ["raw", "qc",  "filled", "filled_by"]
    _best_kind = "filled"  # the kind that is best for simulations
    _ma_cols = []  # the columns in the db to use to calculate the coefficients, 2 values: wi/so or one value:yearly
    # The sign to use to calculate the coefficient and to use the coefficient.
    _coef_sign = ["/", "*"]
    # The multi annual raster to use to calculate the multi annual values
    _ma_raster = RASTERS["dwd_grid"]
    # the postgresql data type of the timestamp column, e.g. "date" or "timestamp"
    _tstp_dtype = None
    _interval = None  # The interval of the timeseries e.g. "1 day" or "10 min"
    _min_agg_to = None # Similar to the interval, but same format ass in AGG_TO
    _agg_fun = "sum" # the sql aggregating function to use

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
            _description_
        """
        if type(self) == StationBase:
            raise NotImplementedError("""
            The StationBase is only a wrapper class an is not working on its own.
            Please use StationN, StationT or StationET instead""")
        self.id = int(id)
        self.id_str = str(id)

        if type(self._ftp_folder_base) == str:
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

    def _check_isin_meta(self):
            if self.isin_meta():
                return True
            else:
                raise NotImplementedError("""
                    The given {para_long} station with id {stid}
                    is not in the corresponding meta table in the DB""".format(
                    stid=self.id, para_long=self._para_long
                ))

    def _check_kind(self, kind):
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

        Raises
        ------
        NotImplementedError
            If the given kind is not valid.
        ValueError
            If the given kind is not a string.
        """
        if type(kind) != str:
            raise ValueError("The given kind is not a string.")

        if kind == "best":
            kind = self._best_kind

        if kind not in self._valid_kinds:
            raise NotImplementedError("""
                The given kind "{kind}" is not a valid kind.
                Must be one of "{valid_kinds}"
                """.format(
                kind=kind,
                valid_kinds='", "'.join(self._valid_kinds)))

        return kind

    def _check_kind_tstp_meta(self, kind):
        """Check if the kind has a timestamp from and until in the meta table."""
        if kind != "last_imp":
            kind = self._check_kind(kind)

        # compute the valid kinds if not already done
        if not hasattr(self, "_valid_kinds_tstp_meta"):
            self._valid_kinds_tstp_meta = ["last_imp"]
            for vk in self._valid_kinds:
                if vk in ["raw", "filled", "corr"]:
                    self._valid_kinds_tstp_meta.append(vk)

        if kind not in self._valid_kinds_tstp_meta:
            raise NotImplementedError("""
                The given kind "{kind}" is not a valid kind.
                Must be one of "{valid_kinds}"
                """.format(
                kind=kind,
                valid_kinds='", "'.join(self._valid_kinds_tstp_meta)))

        return kind

    def _check_kinds(self, kinds):
        """Check if the given kinds are valid.

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
        # check kinds
        if type(kinds) == str:
            kinds = [kinds]
        else:
            kinds = kinds.copy() # because else the original variable is changed

        for i, kind_i in enumerate(kinds):
            if kind_i not in self._valid_kinds:
                kinds[i] = self._check_kind(kind_i)
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
        self._clean_cached_period()
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

        # get period if None providen and compare with filled_period
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        else:
            period = period.copy()

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

    def _clean_cached_period(self):
        time_limit = datetime.now() - timedelta(minutes=1)
        for key in list(self._cached_periods):
            if self._cached_periods[key]["time"] < time_limit:
                self._cached_periods.pop(key)

    @check_superuser
    def _check_ma(self):
        if not self.isin_ma():
            self.update_ma()

    @check_superuser
    def _check_isin_db(self):
        """Check if the station has already a timeserie and if not create one.
        """
        if not self.isin_db():
            self._create_timeseries_table()

    @check_superuser
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        pass

    @check_superuser
    def _expand_timeserie_to_period(self):
        """Expand the timeserie to the complete possible time range"""
        # The interval of 9h and 30 seconds is due to the fact, that the fact that t and et data for the previous day is only updated around 9 on the following day
        # the 10 minutes interval is to get the previous day and not the same day
        sql = """
            WITH whole_ts AS (
                SELECT generate_series(
                    '{min_tstp}'::{tstp_dtype},
                    (SELECT
                        LEAST(
                            date_trunc(
                                'day',
                                min(start_tstp_last_imp) - '9h 30min'::INTERVAL
                            ) - '10 min'::INTERVAL,
                            min(max_tstp_last_imp))
                    FROM para_variables)::{tstp_dtype},
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
            min_tstp=MIN_TSTP.strftime("%Y-%m-%d %H:%M"))
        with DB_ENG.connect()\
                .execution_options(isolation_level="AUTOCOMMIT")\
                as con:
            con.execute(sql)

    @check_superuser
    def _update_db_timeserie(self, df, kinds):
        """Update the timeseries table on the database with new DataFrame.

        Parameters
        ----------
        df : pandas.Series of integers
            A Serie with a DatetimeIndex and the values to update in the width Database.
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
            log.debug(("The _update_db_timeserie methode got an empty df " +
                    "for the {para_long} Station with ID {stid}"
                    ).format(
                para_long=self._para_long,
                stid=self.id))
            return None
        else:
            self._create_timeseries_table()

        with DB_ENG.connect()\
                .execution_options(isolation_level="AUTOCOMMIT")\
                as con:
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
                con.execute(sql_insert)

    @check_superuser
    def _drop(self, why="No reason given"):
        """Drop this station from the database. (meta table and timeseries)
        """
        sql = """
            DROP TABLE IF EXISTS timeseries."{stid}_{para}";
            DELETE FROM meta_{para} WHERE station_id={stid};
            INSERT INTO droped_stations(station_id, para, why, timestamp)
            VALUES ('{stid}', '{para}', '{why}', NOW())
            ON CONFLICT (station_id, para)
                DO UPDATE SET
                    why = EXCLUDED.why,
                    timestamp = EXCLUDED.timestamp;
        """.format(
            stid=self.id, para=self._para,
            why=why.replace("'", "''"))

        with DB_ENG.connect() as con:
            con.execute(sql)
        log.debug(
            "The {para_long} Station with ID {stid} got droped from the database."
            .format(stid=self.id, para_long=self._para_long))

    @check_superuser
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

        with DB_ENG.connect()\
                .execution_options(isolation_level="AUTOCOMMIT")\
                as con:
            con.execute(sql_update)

    @check_superuser
    def _execute_long_sql(self, sql, description="treated"):
        done = False
        attempts = 0
        # execute until done
        while not done:
            attempts += 1
            try:
                with DB_ENG.connect() as con:
                    con.execution_options(isolation_level="AUTOCOMMIT"
                                        ).execute(sql)
                done = True
            except OperationalError as err:
                log_msg = ("There was an operational error for the {para_long} Station (ID:{stid})" +
                        "\nHere is the complete error:\n" + str(err)).format(
                    stid=self.id, para_long=self._para_long)
                if any([
                        True if re.search(
                            "the database system is in recovery mode",
                            arg)
                        else False
                        for arg in err.args]):
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

    @check_superuser
    def _set_is_real(self, state=True):
        sql = """
            UPDATE meta_{para}
            SET is_real={state}
            WHERE station_id={stid};
        """.format(stid=self.id, para=self._para, state=state)

        with DB_ENG.connect() as con:
            con.execute(sql)

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
        with DB_ENG.connect() as con:
            respond = con.execute(sql)

        return respond.first()[0]

    def isin_meta(self):
        """Check if Station is already in the meta table.

        Returns
        -------
        bool
            True if Station is in meta table.
        """
        with DB_ENG.connect() as con:
            result = con.execute("""
            SELECT EXISTS(SELECT station_id FROM meta_{para} WHERE station_id={stid});
            """.format(stid=self.id, para=self._para))
        return result.first()[0]

    def isin_ma(self):
        """Check if Station is already in the multi annual table.

        Returns
        -------
        bool
            True if Station is in multi annual table.
        """

        sql = """
            SELECT
                CASE WHEN {stid} in (select station_id from stations_raster_values)
                     THEN (SELECT {regio_cols_test}
                           FROM stations_raster_values
                           WHERE station_id={stid})
                     ELSE FALSE
                END;
        """.format(
            stid=self.id,
            regio_cols_test=" AND ".join(
                [col + " IS NOT NULL" for col in self._ma_cols]))

        with DB_ENG.connect() as con:
            result = con.execute(sql)
        return result.first()[0]

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
        with DB_ENG.connect() as con:
            res = con.execute(sql)
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

        with DB_ENG.connect() as con:
            res = con.execute(sql)

        return res.first()[0]

    @check_superuser
    def update_period_meta(self, kind):
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

        with DB_ENG.connect() as con:
            con.execute(sql)

    @check_superuser
    def update_ma(self, skip_if_exist=True):
        """Update the multi annual values in the stations_raster_values table.

        Get new values from the raster and put in the table.
        """
        if skip_if_exist and self.isin_ma():
            return None

        # get the srid or proj4
        if "proj4" in self._ma_raster:
            sql_geom = f"ST_SETSRID(ST_TRANSFORM(geometry, '{self._ma_raster['proj4']}'), {self._ma_raster['srid']})"
        else:
            sql_geom = f"ST_TRANSFORM(geometry, {self._ma_raster['srid']})"

        # create sql statement
        sql_new_mas = """
            WITH stat_geom AS (
                SELECT {sql_geom} AS geom
                FROM meta_{para}
                WHERE station_id={stid}
            )
            SELECT {calc_line}
            FROM rasters.{raster_name};
        """.format(
            stid=self.id, para=self._para,
            raster_name=self._ma_raster["db_table"],
            sql_geom=sql_geom,
            calc_line=", ".join(
                ["ST_VALUE(rast, {i}, (SELECT geom FROM stat_geom)) as {name}"
                    .format(
                        i=i, name=self._ma_raster["bands"][i]
                    )

                    for i in range(1, len(self._ma_raster["bands"])+1)])
        )

        with DB_ENG.connect() as con:
            new_mas = con.execute(sql_new_mas).first()

        if any(new_mas):
            # multi annual values were found
            sql_update = """
                INSERT INTO stations_raster_values(station_id, geometry, {ma_cols})
                    Values ({stid}, ST_TRANSFORM('{geom}'::geometry, 25832), {values})
                    ON CONFLICT (station_id) DO UPDATE SET
                    {update};
            """.format(
                stid=self.id,
                ma_cols=', '.join(
                    [str(key) for key in self._ma_raster["bands"].values()]),
                geom=self.get_geom(format=None),
                values=str(new_mas).replace("None", "NULL")[1:-1],
                update=", ".join(
                    ["{key} = EXCLUDED.{key}".format(key=key)
                        for key in self._ma_raster["bands"].values()]
                ))
            with DB_ENG.connect()\
                    .execution_options(isolation_level="AUTOCOMMIT")\
                    as con:
                con.execute(sql_update)
        else:
            # there was no multi annual data found from the raster
            self._drop(
                why="no multi-annual data was found from 'rasters.{raster_name}'"
                .format(raster_name=self._ma_raster["db_table"]))

    def _update_last_imp_period_meta(self, period):
        """Update the meta timestamps for a new import."""
        #check period format
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)

        # update meta file
        # ----------------
        # get last_imp valid kinds that are in the meta file
        last_imp_valid_kinds = self._valid_kinds.copy()
        last_imp_valid_kinds.remove("raw")
        for name in ["qn", "filled_by"]:
            if name in last_imp_valid_kinds:
                last_imp_valid_kinds.remove(name)

        # create update sql
        sql_update_meta = '''
            INSERT INTO meta_{para} as meta
                (station_id, raw_from, raw_until, last_imp_from, last_imp_until
                    {last_imp_cols})
            VALUES ({stid}, {min_tstp}, {max_tstp}, {min_tstp},
                    {max_tstp}{last_imp_values})
            ON CONFLICT (station_id) DO UPDATE SET
                raw_from = LEAST (meta.raw_from, EXCLUDED.raw_from),
                raw_until = GREATEST (meta.raw_until, EXCLUDED.raw_until),
                last_imp_from = CASE WHEN {last_imp_test}
                                    THEN EXCLUDED.last_imp_from
                                    ELSE LEAST(meta.last_imp_from,
                                            EXCLUDED.last_imp_from)
                                    END,
                last_imp_until = CASE WHEN {last_imp_test}
                                    THEN EXCLUDED.last_imp_until
                                    ELSE GREATEST(meta.last_imp_until,
                                                EXCLUDED.last_imp_until)
                                    END
                {last_imp_conflicts};
            '''.format(
            para=self._para,
            stid=self.id,
            last_imp_values=(
                ", " +
                ", ".join(["FALSE"] * len(last_imp_valid_kinds))
            ) if len(last_imp_valid_kinds) > 0 else "",
            last_imp_cols=(
                ", last_imp_" +
                ", last_imp_".join(last_imp_valid_kinds)
            ) if len(last_imp_valid_kinds) > 0 else "",
            last_imp_conflicts=(
                ", last_imp_" +
                " = FALSE, last_imp_".join(last_imp_valid_kinds) +
                " = FALSE"
            ) if len(last_imp_valid_kinds) > 0 else "",
            last_imp_test=(
                "meta.last_imp_" +
                " AND meta.last_imp_".join(last_imp_valid_kinds)
            ) if len(last_imp_valid_kinds) > 0 else "true",
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format_db)))

        # execute meta update
        with DB_ENG.connect()\
                .execution_options(isolation_level="AUTOCOMMIT") as con:
            con.execute(sql_update_meta)

    @check_superuser
    def update_raw(self, only_new=True, ftp_file_list=None):
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

        Returns
        -------
        pandas.DataFrame
            The raw Dataframe of the Stations data.
        """
        zipfiles = self.get_zipfiles(
            only_new=only_new,
            ftp_file_list=ftp_file_list)

        # check for empty list of zipfiles
        if zipfiles is None or len(zipfiles)==0:
            log.debug(
                """raw_update of {para_long} Station {stid}:
                No zipfile was found and therefor no new data was imported."""
                .format(para_long=self._para_long, stid=self.id))
            self._update_last_imp_period_meta(period=(None, None))
            return None

        # download raw data
        df_all = self._download_raw(zipfiles=zipfiles.index)

        # cut out valid time period
        df_all = df_all.loc[df_all.index >= MIN_TSTP]

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
        raw_col = self._cdc_col_names_imp[self._db_col_names_imp.index("raw")]
        selection = selection[~selection[raw_col].isna()]

        # upload to DB
        self._update_db_timeserie(
            selection,
            kinds=self._db_col_names_imp)

        # update raw_files db table
        update_values = \
            ", ".join([str(pair) for pair in zip(
                zipfiles.index,
                zipfiles["modtime"].dt.strftime("%Y%m%d %H:%M").values)]
            )
        with DB_ENG.connect() as con:
            con.execute('''
                INSERT INTO raw_files(filepath, modtime)
                VALUES {values}
                ON CONFLICT (filepath) DO UPDATE SET modtime = EXCLUDED.modtime
                '''.format(values=update_values))

        # if empty skip updating meta filepath
        if len(selection) == 0:
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

        # update meta file
        imp_period = TimestampPeriod(
            selection.index.min(), selection.index.max())
        self._update_last_imp_period_meta(period=imp_period)

        log.info("The raw data for {para_long} station with ID {stid} got updated for the period {min_tstp} to {max_tstp}."
                .format(
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
        # login to CDC FTP server
        CDC.login()

        # check if file list providen
        if ftp_file_list is None:
            ftp_file_list = get_ftp_file_list(
                CDC,
                self._ftp_folders
            )

        # filter for station
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
                 WHERE filepath in ({filepaths})""".format(
                    filepaths="'" +
                    "', '".join(zipfiles_CDC.index.to_list())
                    + "'")
            zipfiles_DB = pd.read_sql(
                sql_db_modtimes, con=DB_ENG
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
            return zipfiles  # .index.to_list()

    def _download_raw(self, zipfiles):
        # download raw data
        # import every file and merge data
        for zf in zipfiles:
            df_new = get_dwd_file(zf)
            df_new.set_index(self._date_col, inplace=True)
            df_new = self._check_df_raw(df_new)

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

        return df_all

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
        return self._download_raw(zipfiles=self.get_zipfiles(only_new=only_new).index)

    @check_superuser
    def _get_sql_new_qc(self, period=(None, None)):
        """Create the SQL statement for the new quality checked data.

        Needs to have one column timestamp and one column qc.

        Parameters
        ----------
        period : util.TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to do the quality check.

        Returns
        -------
        str
            The sql statement for the new quality controlled timeserie.
        """
        pass  # define in the specific classes

    @check_superuser
    def quality_check(self, period=(None, None), **kwargs):
        """Quality check the raw data for a given period.

        Parameters
        ----------
        period : util.TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        """
        period = self._check_period(period=period, kinds=["raw"])

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

        # run commands
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_qc.replace("%%", "%")
        self._execute_long_sql(
            sql=sql_qc,
            description="quality checked for the period {min_tstp} to {max_tstp}.".format(
                **period.get_sql_format_dict(
                    format=self._tstp_format_human)
                ))

        # mark last import as done if in period
        last_imp_period = self.get_last_imp_period()
        if last_imp_period.inside(period):
            self._mark_last_imp_done(kind="qc")

    @check_superuser
    def fillup(self, period=(None, None), **kwargs):
        """Fill up missing data with measurements from nearby stations.

        Parameters
        ----------
        period : util.TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to gap fill the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        """
        self._expand_timeserie_to_period()
        self._check_ma()

        sql_format_dict = dict(
            stid=self.id, para=self._para,
            ma_cols=", ".join(self._ma_cols),
            coef_sign=self._coef_sign,
            base_col="qc" if "qc" in self._valid_kinds else "raw",
            cond_mas_not_null=" OR ".join([
                "ma_other.{ma_col} IS NOT NULL".format(ma_col=ma_col)
                    for ma_col in self._ma_cols]),
            sql_extra_fillup=self._sql_extra_fillup()
        )

        # make condition for period
        if type(period) != TimestampPeriod:
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
        if len(self._ma_cols) == 1:
            sql_format_dict.update(dict(
                is_winter_col="",
                coef_calc="ma_other.{ma_col}{coef_sign[0]}ma_stat.{ma_col}::float AS coef"
                .format(
                    ma_col=self._ma_cols[0],
                    coef_sign=self._coef_sign),
                coef_format="i.coef",
                filled_calc="round(nb.{base_col} {coef_sign[1]} %%3$s, 0)::int"
                .format(**sql_format_dict)
            ))
        elif len(self._ma_cols) == 2:
            sql_format_dict.update(dict(
                is_winter_col=""",
                    CASE WHEN EXTRACT(MONTH FROM timestamp) IN (1, 2, 3, 10, 11, 12)
                        THEN true::bool
                        ELSE false::bool
                    END AS is_winter""",
                coef_calc=(
                    "ma_other.{ma_col[0]}{coef_sign[0]}ma_stat.{ma_col[0]}::float AS coef_wi, \n" + " "*24 +
                    "ma_other.{ma_col[1]}{coef_sign[0]}ma_stat.{ma_col[1]}::float AS coef_so"
                ).format(
                    ma_col=self._ma_cols,
                    coef_sign=self._coef_sign),
                coef_format="i.coef_wi, \n" + " " * 24 + "i.coef_so",
                filled_calc="""
                    CASE WHEN nf.is_winter
                        THEN round(nb.{base_col} {coef_sign[1]} %%3$s, 0)::int
                        ELSE round(nb.{base_col} {coef_sign[1]} %%4$s, 0)::int
                    END""".format(**sql_format_dict)
            ))
        else:
            raise ValueError(
                "There were too many multi annual columns selected. The fillup methode is only implemented for yearly or half yearly regionalisations")

        # Make SQL statement to fill the missing values with values from nearby stations
        sql = """
            CREATE TEMP TABLE new_filled_{stid}_{para}
                ON COMMIT DROP
                AS (SELECT timestamp, {base_col} AS filled,
                        NULL::int AS filled_by {is_winter_col}
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
                        SELECT meta.station_id,
                            meta.raw_from, meta.raw_until,
                            meta.station_id || '_{para}' AS tablename,
                            {coef_calc}
                        FROM meta_{para} meta
                        LEFT JOIN stations_raster_values ma_other
                            ON ma_other.station_id=meta.station_id
                        LEFT JOIN (SELECT {ma_cols}
                                FROM stations_raster_values
                                WHERE station_id = {stid}) ma_stat
                            ON 1=1
                        WHERE meta.station_id != {stid}
                            AND meta.station_id || '_{para}' IN (
                                SELECT tablename
                                FROM pg_catalog.pg_tables
                                WHERE schemaname ='timeseries'
                                    AND tablename LIKE '%%_{para}')
                                AND ({cond_mas_not_null})
                        ORDER BY ST_DISTANCE(
                            geometry_utm,
                            (SELECT geometry_utm
                            FROM meta_{para}
                            WHERE station_id={stid})) ASC)
                    LOOP
                        CONTINUE WHEN i.raw_from > unfilled_period.max
                                      OR i.raw_until < unfilled_period.min;
                        EXECUTE FORMAT(
                        $$
                        UPDATE new_filled_{stid}_{para} nf
                        SET filled={filled_calc},
                            filled_by=%%1$s
                        FROM timeseries.%%2$I nb
                        WHERE nf.filled IS NULL AND nb.{base_col} IS NOT NULL
                            AND nf.timestamp = nb.timestamp;
                        $$,
                        i.station_id,
                        i.tablename,
                        {coef_format}
                        );
                        EXIT WHEN (SELECT SUM((filled IS NULL)::int) = 0
                                   FROM new_filled_{stid}_{para});
                        SELECT min(timestamp) AS min, max(timestamp) AS max
                        INTO unfilled_period
                        FROM new_filled_{stid}_{para}
                        WHERE "filled" IS NULL;
                    END LOOP;
                    {sql_extra_fillup}
                    UPDATE timeseries."{stid}_{para}" ts
                    SET filled = new.filled,
                        filled_by = new.filled_by
                    FROM new_filled_{stid}_{para} new
                    WHERE ts.timestamp = new.timestamp
                        AND ts."filled" IS DISTINCT FROM new."filled";
                END
            $do$;
        """.format(**sql_format_dict)

        # execute
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql.replace("%%", "%")
        self._execute_long_sql(
            sql=sql,
            description="filled for the period {min_tstp} - {max_tstp}".format(
                **period.get_sql_format_dict(format=self._tstp_format_human)))

        # update timespan in meta table
        self.update_period_meta(kind="filled")

        # mark last imp done
        if (("qc" not in self._valid_kinds) or
                (self.is_last_imp_done(kind="qc"))):
            if period.is_empty():
                self._mark_last_imp_done(kind="filled")
            elif period.contains(self.get_last_imp_period()):
                self._mark_last_imp_done(kind="filled")

    @check_superuser
    def _sql_extra_fillup(self):
        """Get the additional sql statement to treat the newly filled temporary table before updating the real timeserie.

        This is mainly for the precipitation Station and returns an empty string for the other stations.

        Returns
        -------
        str
            The SQL statement.
        """
        return ""

    @check_superuser
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

        with DB_ENG.connect() as con:
            con.execute(sql)

    @check_superuser
    def last_imp_quality_check(self):
        """Do the quality check of the last import.
        """
        if not self.is_last_imp_done(kind="qc"):
            self.quality_check(period=self.get_last_imp_period())

    @check_superuser
    def last_imp_qc(self):
        self.last_imp_quality_check()

    @check_superuser
    def last_imp_fillup(self, _last_imp_period=None):
        """Do the gap filling of the last import.
        """
        if not self.is_last_imp_done(kind="filled"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.fillup(period=period)

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
        # check which information to get
        if infos == "all":
            col_clause = ""
        else:
            if type(infos) == str:
                infos = [infos]
            col_clause =" AND column_name IN ({cols})".format(
                cols=", ".join(list(infos)))
        sql = """
        SELECT cols.column_name AS info,
            (SELECT pg_catalog.col_description(c.oid, cols.ordinal_position::int)
             FROM pg_catalog.pg_class c
             WHERE c.oid  = (SELECT cols.table_name::regclass::oid)
               AND c.relname = cols.table_name
            ) as explanation
        FROM information_schema.columns cols
        WHERE cols.table_name = 'meta_{para}'{col_clause};
        """.format(col_clause=col_clause, para=cls._para)

        # get the result
        return pd.read_sql(sql,con=DB_ENG, index_col="info")["explanation"]
        with DB_ENG.connect() as con:
            res = con.execute(sql)
        keys = res.keys()
        values = res.all()
        if len(values)==1:
            return values[0]
        else:
            return dict(zip(keys, values))

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
        if infos == "all":
            cols = "*"
        else:
            if type(infos) == str:
                infos = [infos]
            cols = ", ".join(list(infos))

        # create query
        sql = """
            SELECT {cols} FROM meta_{para} WHERE station_id={stid}
        """.format(
            stid=self.id, para=self._para,
            cols=cols)

        with DB_ENG.connect() as con:
            res = con.execute(sql)
        keys = res.keys()
        values = res.first()
        if len(keys)==1:
            return values[0]
        else:
            return dict(zip(keys, values))

    def get_geom(self, format="EWKT", crs=None):
        """Get the point geometry of the station.

        Parameters
        ----------
        format: str or None, optional
            The format of the geometry to return.
            Needs to be a format that is understood by Postgresql.
            ST_AsXXXXX function needs to exist in postgresql language.
            If None, then the binary representation is returned.
            the default is "EWKT".
        crs: str, int or None, optional
            If None, then the geometry is returned in WGS84 (EPSG:4326).
            If string, then it should be one of "WGS84" or "UTM".
            If int, then it should be the EPSG code.

        Returns
        -------
        str or bytes
            string or bytes representation of the geometry,
            depending on the selected format.
        """
        # change WKT to Text, because Postgis function is ST_AsText for WKT
        if format == "WKT":
            format = "Text"

        # check crs
        utm=""
        trans_fun=""
        epsg=""
        if type(crs)==str:
            if crs.lower() == "utm":
                utm="_utm"
        elif type(crs)==int:
            trans_fun="ST_TRANSFORM("
            epsg=", {0})".format(crs)
        # get the geom
        return self.get_meta(
            infos=[
                "{trans_fun}{st_fun}(geometry{utm}){epsg}".format(
                    st_fun="ST_As" + format if format else "",
                    utm=utm,
                    trans_fun=trans_fun,
                    epsg=epsg)])

    def get_geom_shp(self, crs=None):
        """Get the geometry of the station as a shapely Point object.

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
        return shapely.wkt.loads(self.get_geom("Text", crs=crs))

    def get_name(self):
        return self.get_meta(infos="stationsname")

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
        TimespanPeriod:
            The TimespanPeriod of the station or of all the stations if all=True.

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

        with DB_ENG.connect() as con:
            res = con.execute(sql)

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
        util.TimestampPeriod
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
            with DB_ENG.connect() as con:
                respond = con.execute(sql)

            return TimestampPeriod(*respond.first())
        else:
            return TimestampPeriod(None, None)

    def get_max_period(self, kinds, nas_allowed=False):
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
        utils.TimestampPeriod
            The maximum Timestamp Period
        """
        if nas_allowed:
            sql_max_tstp = """
                SELECT MIN("timestamp"), MAX("timestamp")
                FROM timeseries."{stid}_{para}";
                """.format(
                    stid=self.id, para=self._para)
            with DB_ENG.connect() as con:
                res = con.execute(sql_max_tstp)
            max_period = TimestampPeriod(*res.first())
        else:
            kinds = self._check_kinds(kinds)
            if len(kinds)>0:
                max_period = self.get_filled_period(kind=kinds[0])
                for kind in kinds[1:]:
                    max_period = max_period.union(
                        self.get_filled_period(kind=kind),
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

    def get_neighboor_stids(self, n=5, only_real=True):
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

        Returns
        -------
        list of int
            A list of station Ids in order of distance.
            The closest station is the first in the list.
        """
        self._check_isin_meta()

        sql_nearest_stids = """
                SELECT station_id
                FROM meta_{para}
                WHERE station_id != {stid} {cond_only_real}
                ORDER BY ST_DISTANCE(
                    geometry_utm,
                    (SELECT geometry_utm FROM meta_{para}
                     WHERE station_id={stid}))
                LIMIT {n}
            """.format(
            cond_only_real="AND is_real" if only_real else "",
            stid=self.id, para=self._para, n=n)
        with DB_ENG.connect() as con:
            result = con.execute(sql_nearest_stids)
            nearest_stids = [res[0] for res in result.all()]
        return nearest_stids

    def get_multi_annual(self):
        """Get the multi annual value(s) for this station.

        Returns
        -------
        list or number
            The corresponding multi annual value.
            For T en ET the yearly value is returned.
            For N the winter and summer half yearly sum is returned in tuple.
        """
        sql = """
            SELECT {ma_cols}
            FROM stations_raster_values
            WHERE station_id = {stid}
        """.format(
            ma_cols=", ".join(self._ma_cols),
            stid=self.id
        )

        with DB_ENG.connect() as con:
            res = con.execute(sql).first()

        # Update ma values if no result returned
        if res is None:
            self.update_ma()
            with DB_ENG.connect() as con:
                res = con.execute(sql).first()

        if res is None:
            return None
        else:
            return list(res)

    def get_ma(self):
        return self.get_multi_annual()

    def get_raster_value(self, raster):
        if type(raster) == str:
            raster = RASTERS[raster]

        sql = """
            WITH stat_geom AS (
                SELECT ST_TRANSFORM(geometry, {raster_srid}) AS geom
                FROM meta_{para}
                WHERE station_id={stid}
            )
            SELECT ST_VALUE(rast, 1, (SELECT geom FROM stat_geom)) as slope
            FROM rasters.{raster_name}
            WHERE ST_INTERSECTS(rast, (SELECT geom FROM stat_geom));
        """.format(
            stid=self.id, para=self._para,
            raster_name=raster["db_table"],
            raster_srid=raster["srid"]
        )
        # return sql
        with DB_ENG.connect() as con:
            value = con.execute(sql).first()[0]

        return raster["dtype"](value)

    def get_coef(self, other_stid):
        """Get the regionalisation coefficients due to the height.

        Those are the values from the dwd grid or regnie grids.

        Parameters
        ----------
        other_stid : int
            The Station Id of the other station from wich to regionalise for own station.

        Returns
        -------
        list of floats or None
            A list of
        """
        ma_values = self.get_multi_annual()
        other_stat = self.__class__(other_stid)
        other_ma_values = other_stat.get_multi_annual()

        if other_ma_values is None or ma_values is None:
            return None
        else:
            if self._coef_sign[0] == "/":
                return [own/other for own, other in zip(ma_values, other_ma_values)]
            elif self._coef_sign[0] == "-":
                return [own-other for own, other in zip(ma_values, other_ma_values)]
            else:
                return None

    def get_df(self, kinds, period=(None, None), agg_to=None,
               nas_allowed=True, add_na_share=False, db_unit=False):
        """Get a timeseries DataFrame from the database.

        Parameters
        ----------
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by".
            For the precipitation also "qn" and "corr" are valid.
            If "filled_by" is given together with an aggregation step, the "filled_by" is replaced by the "filled_share".
            The "filled_share" gives the share of filled values in the aggregation group in percent.
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        agg_to : str or None, optional
            Aggregate to a given timespan.
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
        kinds = self._check_kinds(kinds=kinds)
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
            else:
                add_filled_share = False

            # create sql parts
            kinds_before = kinds.copy()
            kinds = [
                "{agg_fun}({kind}) AS {kind}".format(
                    agg_fun=self._agg_fun, kind=kind) for kind in kinds]
            timestamp_col = "date_trunc('{agg_to}', timestamp)".format(
                agg_to=agg_to)
            group_by = "GROUP BY " + timestamp_col

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

        # create base sql
        sql = """
            SELECT {timestamp_col} as timestamp, {kinds}
            FROM timeseries."{stid}_{para}"
            WHERE timestamp BETWEEN {min_tstp} AND {max_tstp}
            {group_by}
            ORDER BY timestamp ASC;
            """.format(
                stid=self.id,
                para=self._para,
                kinds=', '.join(kinds),
                group_by=group_by,
                timestamp_col=timestamp_col,
                **period.get_sql_format_dict(
                    format="'{}'".format(self._tstp_format_db))
            )

        df = pd.read_sql(sql, con=DB_ENG, index_col="timestamp")

        # convert filled_by to Int16, pandas Integer with NA support
        if "filled_by" in kinds:
            df["filled_by"] = df["filled_by"].astype("Int16")

        # change index to pandas DatetimeIndex if necessary
        if type(df.index) != pd.DatetimeIndex:
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
        kwargs : dict, optional
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
        kwargs : dict, optional
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

        df = pd.read_sql(
            sql, con=DB_ENG, index_col="timestamp")

        # change index to pandas DatetimeIndex if necessary
        if type(df.index) != pd.DatetimeIndex:
            df.set_index(pd.DatetimeIndex(df.index), inplace=True)

        return df

    def get_filled(self, period=(None, None), with_dist=False):
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
        df = self.get_df(period=period, kinds="filled")

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
        kwargs : dict, optional
            The keyword arguments are passed to the get_df function.
            Possible parameters are "period", "agg_to" or "nas_allowed".

        Returns
        -------
        pandas.DataFrame
            A timeserie with the adjusted data.
        """
        # this is only the first part of the methode
        # get basic values
        main_df = self.get_df(
            kinds=["filled"], # not best, as the ma values are not richter corrected
            **kwargs)
        ma = self.get_multi_annual()

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
        df = self.get_df(kinds=[kind], period=period, db_unit=False, agg_to=agg_to)

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
            elif self.isin_meta_n():
                self._create_timeseries_table()
                return True
        elif self.isin_meta_n():
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
             FROM meta_n
             WHERE station_id = {stid})
        """.format(stid=self.id, para=self._para)

        with DB_ENG.connect().execution_options(isolation_level="AUTOCOMMIT") as con:
            con.execute(sql)

    def isin_meta_n(self):
        """Check if Station is in the precipitation meta table.

        Returns
        -------
        bool
            True if Station is in the precipitation meta table.
        """
        with DB_ENG.connect() as con:
            result = con.execute("""
            SELECT {stid} in (SELECT station_id FROM meta_n);
            """.format(stid=self.id))
        return result.first()[0]

    def quality_check(self, period=(None, None)):
        if not self.is_virtual():
            super().quality_check(period=period)


class StationTETBase(StationCanVirtualBase):
    """A base class for T and ET.

    This class adds methods that are only used by temperatur and evapotranspiration stations.
    """
    _tstp_dtype = "date"
    _interval = "1 day"
    _min_agg_to = "day"
    _tstp_format_db = "%Y%m%d"
    _tstp_format_human = "%Y-%m-%d"

    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp date PRIMARY KEY,
                raw int4,
                qc int4,
                filled int4,
                filled_by int2
                );
        '''.format(stid=self.id, para=self._para)
        with DB_ENG.connect() as con:
            con.execute(sql_add_table)

    def _get_sql_near_mean(self, period, only_real=True):
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

        Returns
        -------
        str
            SQL statement for the regionalised mean of the 5 nearest stations.
        """
        near_stids = self.get_neighboor_stids(n=5, only_real=only_real)
        coefs = [self.get_coef(other_stid=near_stid)[0]
                 for near_stid in near_stids]
        coefs = ["NULL" if coef is None else coef for coef in coefs]

        # create sql for mean of the near stations and the raw value itself
        sql_near_mean = """
            SELECT ts.timestamp,
                (COALESCE(ts1.qc {coef_sign[1]} {coefs[0]}, 0) +
                 COALESCE(ts2.qc {coef_sign[1]} {coefs[1]}, 0) +
                 COALESCE(ts3.qc {coef_sign[1]} {coefs[2]}, 0) +
                 COALESCE(ts4.qc {coef_sign[1]} {coefs[3]}, 0) +
                 COALESCE(ts5.qc {coef_sign[1]} {coefs[4]}, 0) )
                 / (NULLIF(5 - (
                        (ts1.qc IS NULL OR {coefs[0]} is NULL)::int +
                        (ts2.qc IS NULL OR {coefs[1]} is NULL)::int +
                        (ts3.qc IS NULL OR {coefs[2]} is NULL)::int +
                        (ts4.qc IS NULL OR {coefs[3]} is NULL)::int +
                        (ts5.qc IS NULL OR {coefs[4]} is NULL)::int ) , 0)
                 ) AS mean,
                ts."raw" as raw
            FROM timeseries."{stid}_{para}" AS ts
            LEFT JOIN timeseries."{near_stids[0]}_{para}" ts1
                ON ts.timestamp=ts1.timestamp
            LEFT JOIN timeseries."{near_stids[1]}_{para}" ts2
                ON ts.timestamp=ts2.timestamp
            LEFT JOIN timeseries."{near_stids[2]}_{para}" ts3
                ON ts.timestamp=ts3.timestamp
            LEFT JOIN timeseries."{near_stids[3]}_{para}" ts4
                ON ts.timestamp=ts4.timestamp
            LEFT JOIN timeseries."{near_stids[4]}_{para}" ts5
                ON ts.timestamp=ts5.timestamp
            WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
            """.format(
            stid=self.id,
            para=self._para,
            near_stids=near_stids,
            coefs=coefs,
            coef_sign=self._coef_sign,
            **period.get_sql_format_dict()
        )

        return sql_near_mean

    def get_adj(self, **kwargs):
        """Get the adjusted timeserie.

        The timeserie get adjusted to match the multi-annual value over the given period.
        So the yearly variability is kept and only the whole period is adjusted.

        Returns
        -------
        pd.DataFrame
            The adjusted timeserie with the timestamp as index.
        """        
        # this is only the second part of the methode
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


class StationNBase(StationBase):
    _date_col = "MESS_DATUM"
    _decimals = 100
    _ma_cols = ["n_regnie_wihj", "n_regnie_sohj"]
    _ma_raster = RASTERS["regnie_grid"]

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
        # sohj
        sohj_months = [4, 5, 6, 7, 8, 9]
        mask_sohj = main_df.index.month.isin(sohj_months)

        main_df_sohj = main_df[mask_sohj]

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
                raise ValueError("The get_adj methode does not work on decade values.")

        main_df_sohj_y = main_df_sohj.groupby(main_df_sohj.index.year)\
            .sum(min_count=min_count).mean()

        adj_df[mask_sohj] = (main_df_sohj * (ma[1] / main_df_sohj_y)).round(2)

        # wihj
        mask_wihj = ~mask_sohj
        main_df_wihj = main_df[mask_wihj]
        main_df_wihj_y = main_df_wihj.groupby(main_df_wihj.index.year)\
            .sum(min_count=min_count).mean()
        adj_df[mask_wihj] = (main_df_wihj * (ma[0] / main_df_wihj_y)).round(2)

        return adj_df


# the different Station kinds:
class StationN(StationNBase):
    """A class to work with and download 10 minutes precipitation data for one station."""
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/"]
    _para = "n"
    _para_long = "Precipitation"
    _cdc_col_names_imp = ["RWS_10", "QN"]
    _db_col_names_imp = ["raw", "qn"]
    _tstp_format_db = "%Y%m%d %H:%M"
    _tstp_dtype = "timestamp"
    _interval = "10 min"
    _min_agg_to = "10 min"
    _unit = "mm/10min"
    _valid_kinds = ["raw", "qn", "qc", "corr", "filled", "filled_by"]
    _best_kind = "corr"

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = dwd_id_to_str(id)

    def _get_sql_new_qc(self, period):
        # create sql_format_dict
        sql_format_dict = dict(
            para=self._para, stid=self.id, para_long=self._para_long,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format_db)),
            limit=0.1*self._decimals) # don't delete values below 0.1mm/10min if they are consecutive

        # check if daily station is available
        sql_check_d = """
            SELECT EXISTS(
                SELECT *
                FROM information_schema.tables
                WHERE table_schema = 'timeseries'
                    AND table_name = '{stid}_{para}_d'
            );""".format(**sql_format_dict)
        with DB_ENG.connect() as con:
            daily_exists = con.execute(sql_check_d).first()[0]

        # create sql for dates where the aggregated 10 minutes measurements are 0
        # althought the daily measurements are not 0
        if daily_exists:
            sql_dates_failed = """
                WITH ts_10min_d AS (
                    SELECT (ts.timestamp - INTERVAL '5h 50 min')::date as date, sum("raw") as raw
                    FROM timeseries."{stid}_{para}" ts
                    WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
                    GROUP BY (ts.timestamp - INTERVAL '5h 50 min')::date)
                SELECT date
                FROM timeseries."{stid}_{para}_d" ts_d
                LEFT JOIN ts_10min_d ON ts_d.timestamp::date=ts_10min_d.date
                WHERE ts_d.timestamp BETWEEN {min_tstp}::date AND {max_tstp}::date
                    AND ts_10min_d.raw = 0 AND ts_d.raw <> 0
            """.format(**sql_format_dict)
        else:
            log.warn((
                "For the {para_long} station with ID {stid} there is no timeserie with daily values. " +
                "Therefor the quality check for daily values equal to 0 is not done.\n" +
                "Please consider updating the daily stations with:\n" +
                "stats = stations.StationNDs()\n" +
                "stats.update_meta()\nstats.update_raw()"
            ).format(**sql_format_dict))
            sql_dates_failed = """
                SELECT NULL::date as date
            """

        # make sql for timestamps where 3 times same value in a row
        sql_tstps_failed = """
            WITH tstps_df as (
                SELECT  ts.timestamp as tstp_1,
                        ts2.timestamp as tstp_2,
                        ts3.timestamp as tstp_3
                from timeseries."{stid}_{para}" ts
                INNER JOIN timeseries."{stid}_{para}" ts2
                    on ts.timestamp = ts2.timestamp - INTERVAL '10 min'
                INNER JOIN timeseries."{stid}_{para}" ts3
                    on ts.timestamp = ts3.timestamp - INTERVAL '20 min'
                WHERE ts.qn != 3
                    AND ts.raw = ts2.raw AND ts2.raw = ts3.raw
                    AND ts.raw > {limit:n}
                    AND ts.raw is not NULL
                    AND ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
            )
            SELECT tstp_1 AS timestamp FROM tstps_df
            UNION SELECT tstp_2 FROM tstps_df
            UNION SELECT tstp_3 FROM tstps_df
        """.format(**sql_format_dict)

        sql_new_qc = """
            WITH tstps_failed as ({sql_tstps_failed}),
                    dates_failed AS ({sql_dates_failed})
            SELECT ts.timestamp,
                (CASE WHEN ((ts.timestamp IN (SELECT timestamp FROM tstps_failed))
                        OR ((ts.timestamp - INTERVAL '5h 50 min')::date IN (
                            SELECT date FROM dates_failed))
                        OR ts."raw" < 0)
                    THEN NULL
                    ELSE ts."raw" END) as qc
            FROM timeseries."{stid}_{para}" ts
            WHERE ts.timestamp BETWEEN {min_tstp} AND {max_tstp}
        """.format(
            sql_tstps_failed=sql_tstps_failed,
            sql_dates_failed=sql_dates_failed,
            **sql_format_dict)

        return sql_new_qc

    def _check_df_raw(self, df):
        """This function is used in the Base class on the single dataframe that is downloaded from the CDC Server before loading it in the database.

        Here the function adapts the timezone relative to the date.
        As the data on the CDC server is in MEZ before 200 and in UTC after 2000
        
        Some precipitation stations on the DWD CDC server have also rows outside of the normal 10 Minute frequency, e.g. 2008-09-16 01:47 for Station 662.
        Because those rows only have NAs for the measurement they are deleted."""
        # correct Timezone before 2000 MEZ after UTC
        mask_before_2000 = df.index < pd.Timestamp(2000,1,1,0,0)
        if any(mask_before_2000):
            df_before_2000 = df[mask_before_2000].copy()
            df_before_2000.index = df_before_2000.index.tz_localize("Etc/GMT+1")\
                                    .tz_convert("UTC")
            df_after_2000 = df[~mask_before_2000].copy()
            df_after_2000.index = df_after_2000.index.tz_localize("UTC")
            df = pd.concat([df_before_2000, df_after_2000])
        else:
            df.index = df.index.tz_localize("UTC")

        # delete measurements outside of the 10 minutes frequency
        df = df[df.index.minute%10==0].copy()

        return df

    @check_superuser
    def _create_timeseries_table(self):
        """Create the timeseries table in the DB if it is not yet existing."""
        sql_add_table = '''
            CREATE TABLE IF NOT EXISTS timeseries."{stid}_{para}"  (
                timestamp timestamp PRIMARY KEY,
                raw int4,
                qn smallint,
                qc  int4,
                filled int4,
                filled_by int2,
                corr int4
            );
        '''.format(stid=self.id, para=self._para)
        with DB_ENG.connect() as con:
            con.execute(sql_add_table)

    @staticmethod
    def _richter_class_from_horizon(horizon):
        richter_class = None
        for key in RICHTER_CLASSES:
            if (horizon >= RICHTER_CLASSES[key]["min_horizon"]) and \
                (horizon < RICHTER_CLASSES[key]["max_horizon"]):
                richter_class = key
        return richter_class

    @check_superuser
    def update_horizon(self, skip_if_exist=True):
        """Update the horizon angle (Horizontabschirmung) in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True.

        Returns
        -------
        float
            The horizon angle in degrees (Horizontabschirmung).
        """
        if skip_if_exist:
            horizon = self.get_horizon()
            if horizon is not None:
                return horizon

        # check if files are available
        for dgm_name in ["dgm5", "dgm80"]:
            if not RASTERS["local"][dgm_name].is_file():
                raise ValueError(
                    "The {dgm_name} was not found in the data directory under: \n{fp}".format(
                        dgm_name=dgm_name,
                        fp=str(RASTERS["local"][dgm_name])
                    )
                )

        # get the horizon value
        radius = 75000 # this value got defined because the maximum height is around 4000m for germany
        with rio.open(RASTERS["local"]["dgm5"]) as dgm5,\
             rio.open(RASTERS["local"]["dgm80"]) as dgm80:
                geom = self.get_geom_shp(crs="utm")
                xy = [geom.x, geom.y]
                # sample station heght
                stat_h = list(dgm5.sample(
                    xy=[xy],
                    indexes=1,
                    masked=True))[0]
                if stat_h.mask[0]:
                    log.error("update_horizon(): No height was found for {para_long} Station {stid}. Therefor the horizon angle could not get updated.".format(
                        stid=self.id, para_long=self._para_long))
                    return None
                else:
                    stat_h = stat_h[0]

                # sample dgm for horizon angle
                hab = pd.Series(
                    index=pd.Index([], name="angle", dtype=int),
                    name="horizon")
                for angle in range(90, 271, 3):
                    dgm5_mask = polar_line(xy, radius, angle)
                    dgm5_np, dgm5_tr = rasterio.mask.mask(
                        dgm5, [dgm5_mask], crop=True)
                    dgm5_np[dgm5_np==dgm5.profile["nodata"]] = np.nan
                    dgm_gpd = raster2points(dgm5_np, dgm5_tr, crs=dgm5.crs)
                    dgm_gpd["dist"] = dgm_gpd.distance(Point(xy))

                    # check if parts are missing and fill
                    #####################################
                    dgm_gpd = dgm_gpd.sort_values("dist").reset_index(drop=True)
                    line_parts = pd.DataFrame(
                        columns=["Start_point", "radius", "line"])
                    # look for holes inside the line
                    for j in dgm_gpd[dgm_gpd["dist"].diff() > 10].index:
                        line_parts = line_parts.append(
                            {"Start_point": dgm_gpd.loc[j-1, "geometry"],
                             "radius": dgm_gpd.loc[j, "dist"] - dgm_gpd.loc[j-1, "dist"]},
                            ignore_index=True)

                    # look for missing values at the end
                    dgm5_max_dist = dgm_gpd.iloc[-1]["dist"]
                    if dgm5_max_dist < (radius - 5):
                            line_parts = line_parts.append(
                                {"Start_point": dgm_gpd.iloc[-1]["geometry"],
                                 "radius": radius - dgm5_max_dist},
                                ignore_index=True)

                    # check if parts are missing and fill
                    if len(line_parts) > 0:
                            # create the lines
                            for i, row in line_parts.iterrows():
                                line_parts.loc[i, "line"] = polar_line(
                                    [el[0] for el in row["Start_point"].xy],
                                    row["radius"],
                                    angle
                                )
                            dgm80_mask = MultiLineString(
                                line_parts["line"].tolist())
                            dgm80_np, dgm80_tr = rasterio.mask.mask(
                                dgm80, [dgm80_mask], crop=True)
                            dgm80_np[dgm80_np==dgm80.profile["nodata"]] = np.nan
                            dgm80_gpd = raster2points(
                                dgm80_np, dgm80_tr, crs=dgm80.crs
                                ).to_crs(dgm5.crs)
                            dgm80_gpd["dist"] = dgm80_gpd.distance(Point(xy))
                            dgm_gpd = dgm_gpd.append(
                                dgm80_gpd, ignore_index=True)

                    hab[angle] = np.max(np.degrees(np.arctan(
                            (dgm_gpd["data"]-stat_h) / dgm_gpd["dist"])))

                # calculate the mean "horizontabschimung"
                # Richter: H’=0,15H(S-SW) +0,35H(SW-W) +0,35H(W-NW) +0, 15H(NW-N)
                horizon = max(0,
                    0.15*hab[(hab.index>225) & (hab.index<=270)].mean()
                    + 0.35*hab[(hab.index>=180) & (hab.index<=225)].mean()
                    + 0.35*hab[(hab.index>=135) & (hab.index<180)].mean()
                    + 0.15*hab[(hab.index>=90) & (hab.index<135)].mean())

                # insert to meta table in db
                self._update_meta(
                    cols=["horizon"],
                    values=[horizon])

                return horizon

    @check_superuser
    def update_richter_class(self, skip_if_exist=True):
        """Update the richter class in the meta table.

        Get new values from the raster and put in the table.

        Parameters
        ----------
        skip_if_exist : bool, optional
            Skip updating the value if there is already a value in the meta table.
            The default is True

        Returns
        -------
        str
            The richter class name.
        """
        # check if already value in table
        if skip_if_exist:
            richter_class = self.get_richter_class()
            if self.get_richter_class() is not None:
                return richter_class

        # get the richter class
        richter_class = self._richter_class_from_horizon(
            horizon=self.update_horizon(skip_if_exist=skip_if_exist))

        # save to db
        self._update_meta(
                cols=["richter_class"],
                values=[richter_class])

        return richter_class

    @check_superuser
    def richter_correct(self, period=(None, None), **kwargs):
        """Do the richter correction on the filled data for the given period.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).

        Raises
        ------
        Exception
            If no richter class was found for this station.
        """
        # check if period is given
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        period_in = period.copy()
        if not period.is_empty():
            period = self._check_period(
                period=period, kinds=["filled"])
            sql_period_clause = """
                WHERE timestamp BETWEEN {min_tstp} AND {max_tstp}
            """.format(
                **period.get_sql_format_dict(
                    format="'{}'".format(self._tstp_format_db)
                )
            )
        else:
            sql_period_clause = ""

        # check if temperature station is filled
        stat_t = StationT(self.id)
        stat_t_period = stat_t.get_filled_period(kind="filled")
        stat_n_period = self.get_filled_period(kind="filled")
        delta = timedelta(hours=5, minutes=50)
        min_date = pd.Timestamp(MIN_TSTP).date()
        stat_t_min = stat_t_period[0].date()
        stat_t_max = stat_t_period[1].date()
        stat_n_min = (stat_n_period[0] - delta).date()
        stat_n_max = (stat_n_period[1] - delta).date()
        if stat_t_period.is_empty()\
                or (stat_t_min > stat_n_min
                    and not (stat_n_min < min_date)
                            and (stat_t_min == min_date)) \
                or (stat_t_max  < stat_n_max)\
                and not stat_t.is_last_imp_done(kind="filled"):
            stat_t.fillup(period=period)

        # get the richter exposition class
        richter_class = self.update_richter_class(skip_if_exist=True)
        if richter_class is None:
            raise Exception("No richter class was found for the precipitation station {stid} and therefor no richter correction was possible."\
                            .format(stid=self.id))

        # create the sql queries
        sql_format_dict = dict(
            stid=self.id,
            para=self._para,
            richter_class=richter_class,
            period_clause=sql_period_clause,
            n_decim=self._decimals,
            t_decim=stat_t._decimals
        )
        # daily precipitation
        sql_n_daily = """
            SELECT timestamp::date AS date,
                   sum("filled") AS "filled",
                   count(*) FILTER (WHERE "filled" > 0) AS "count_n"
            FROM timeseries."{stid}_{para}"
            {period_clause}
            GROUP BY timestamp::date
        """.format(**sql_format_dict)

        # add is_winter
        sql_n_daily_winter = """
            SELECT date,
			    CASE WHEN EXTRACT(MONTH FROM date) IN (1, 2, 3, 10, 11, 12)
			            THEN true::bool
			            ELSE false::bool
			    END AS is_winter,
			    "filled" AS "n_d",
			    "count_n"
			FROM ({sql_n_daily}) tsn_d
        """.format(sql_n_daily=sql_n_daily)

        # add precipitation class
        sql_n_daily_precip_class = """
            SELECT
                date, "count_n", "n_d",
                CASE WHEN (tst."filled" >= (3 * {t_decim}) AND "is_winter") THEN 'precip_winter'
                    WHEN (tst."filled" >= (3 * {t_decim}) AND NOT "is_winter") THEN 'precip_summer'
                    WHEN (tst."filled" <= (-0.7 * {t_decim})::int) THEN 'snow'
                    WHEN (tst."filled" IS NULL) THEN NULL
                    ELSE 'mix'
                END AS precipitation_typ
            FROM ({sql_n_daily_winter}) tsn_d_wi
            LEFT JOIN timeseries."{stid}_t" tst
                ON tst.timestamp=tsn_d_wi.date
        """.format(
            sql_n_daily_winter=sql_n_daily_winter,
            **sql_format_dict
        )

        # calculate the delta n
        sql_delta_n = """
            SELECT date,
                CASE WHEN "count_n"> 0 THEN
                        round(("b_{richter_class}" * ("n_d"::float/{n_decim})^"E" * {n_decim})/"count_n")
                    ELSE 0
                END AS "delta_10min"
            FROM ({sql_n_daily_precip_class}) tsn_d2
            LEFT JOIN richter_values r
                ON r."precipitation_typ"=tsn_d2."precipitation_typ"
        """.format(
            sql_n_daily_precip_class=sql_n_daily_precip_class,
            **sql_format_dict
        )

        # calculate the new corr
        sql_update = """
            UPDATE timeseries."{stid}_{para}" ts
            SET "corr" = CASE WHEN "filled" > 0
                            THEN ts."filled" + ts_delta_n."delta_10min"
                            ELSE ts."filled"
                            END
            FROM ({sql_delta_n}) ts_delta_n
            WHERE (ts.timestamp)::date = ts_delta_n.date;
        """.format(
            sql_delta_n=sql_delta_n,
            **sql_format_dict
        )

        # run commands
        if "return_sql" in kwargs and kwargs["return_sql"]:
            return sql_update.replace("%%", "%")

        self._execute_long_sql(
            sql_update,
            description="richter corrected for the period {min_tstp} - {max_tstp}".format(
                **period.get_sql_format_dict(format=self._tstp_format_human)
            ))

        # mark last import as done, if previous are ok
        if (self.is_last_imp_done(kind="qc") and self.is_last_imp_done(kind="filled")):
            if (period_in.is_empty() or
                period_in.contains(self.get_last_imp_period())):
                self._mark_last_imp_done(kind="corr")

        # calculate the difference to filled timeserie
        if period.is_empty() or period[0].year < pd.Timestamp.now().year:
            sql_diff_filled = """
                UPDATE meta_n
                SET quot_corr_filled = quot_avg
                FROM (
                    SELECT avg(quot)*100 AS quot_avg
                    FROM (
                        SELECT sum("corr")::float/sum("filled")::float AS quot
                        FROM timeseries."{stid}_{para}"
                        GROUP BY date_trunc('year', "timestamp")
                        HAVING count("filled") > 364 * 6 *24) df_y) df_avg
                WHERE station_id={stid};""".format(**sql_format_dict)

            with DB_ENG.connect() as con:
                con.execute(sql_diff_filled)

        # update filled time in meta table
        self.update_period_meta(kind="corr")

    @check_superuser
    def corr(self, *args, **kwargs):
        return self.richter_correct(*args, **kwargs)

    @check_superuser
    def last_imp_richter_correct(self, _last_imp_period=None):
        """Do the richter correction of the last import.

        Parameters
        ----------
        _last_imp_period : _type_, optional
            Give the overall period of the last import.
            This is only for intern use of the stationsN methode to not compute over and over again the period.
            The default is None.
        """
        if not self.is_last_imp_done(kind="corr"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.richter_correct(
                period=period)

        else:
            log.info("The last import of {para_long} Station {stid} was already richter corrected and is therefor skiped".format(
                stid=self.id, para_long=self._para_long
            ))

    @check_superuser
    def last_imp_corr(self, _last_imp_period=None):
        """A wrapper for last_imp_richter_correct()."""
        return self.last_imp_richter_correct(_last_imp_period=_last_imp_period)

    @check_superuser
    def _sql_extra_fillup(self):
        sql_extra = """
            UPDATE new_filled_{stid}_{para} ts
            SET filled = filled * coef
            FROM (
                SELECT
                    date,
                    ts_d."raw"/ts_10."filled"::float AS coef
                FROM (
                    SELECT
                        date(timestamp - '5h 50min'::INTERVAL),
                        sum(filled) AS filled
                    FROM new_filled_{stid}_{para}
                    GROUP BY date(timestamp - '5h 50min'::INTERVAL)
                    ) ts_10
                LEFT JOIN timeseries."{stid}_n_d" ts_d
                    ON ts_10.date=ts_d.timestamp
                WHERE ts_d."raw" IS NOT NULL
                      AND ts_10.filled > 0
                ) df_coef
            WHERE (ts.timestamp - '5h 50min'::INTERVAL)::date = df_coef.date
                AND coef != 1;
        """.format(stid=self.id, para=self._para)

        return sql_extra

    @check_superuser
    def fillup(self, period=(None, None), **kwargs):
        super_ret = super().fillup(period=period, **kwargs)

        # check the period
        if type(period) != TimestampPeriod:
            period= TimestampPeriod(*period)

        # update difference to regnie
        if period.is_empty() or period[0].year < pd.Timestamp.now().year:
            sql_diff_ma = """
                UPDATE meta_n
                SET quot_filled_regnie = quots.quot_regnie*100,
                    quot_filled_dwd_grid = quots.quot_dwd*100
                FROM (
                    SELECT df_ma.ys / (srv.n_regnie_year*{decimals}) AS quot_regnie,
                        df_ma.ys / (srv.n_year*{decimals}) AS quot_dwd
                    FROM (
                        SELECT avg(df_a.yearly_sum) as ys
                        FROM (
                            SELECT sum("filled") AS yearly_sum
                            FROM timeseries."{stid}_{para}"
                            GROUP BY date_trunc('year', "timestamp")
                            HAVING count("filled") > 363 * 6 * 24) df_a
                        ) df_ma
                    LEFT JOIN stations_raster_values srv
                        ON station_id={stid}) quots
                WHERE station_id ={stid};
            """.format(
                stid=self.id,
                para=self._para,
                decimals=self._decimals)

            #execute sql or return
            if "return_sql" in kwargs and kwargs["return_sql"]:
                return (str(super_ret) + "\n" + sql_diff_ma).replace("%%", "%")
            with DB_ENG.connect() as con:
                con.execute(sql_diff_ma)

    def get_corr(self, **kwargs):
        return self.get_df(kinds=["corr"], **kwargs)

    def get_qn(self, **kwargs):
        return self.get_df(kinds=["qn"], **kwargs)

    def get_richter_class(self, update_if_fails=True):
        """Get the richter class for this station.

        Provide the data from the meta table.

        Parameters
        ----------
        update_if_fails: bool, optional
            Should the richter class get updatet if no exposition class is found in the meta table?
            If False and no exposition class was found None is returned.
            The default is True.

        Returns
        -------
        string
            The corresponding richter exposition class.
        """
        sql = """
            SELECT richter_class
            FROM meta_{para}
            WHERE station_id = {stid}
        """.format(stid=self.id, para=self._para)

        with DB_ENG.connect() as con:
            res = con.execute(sql).first()

        # check result
        if res is None:
            if update_if_fails:
                if DB_ENG.is_superuser:
                    self.update_richter_class()
                    # update_if_fails is False to not get an endless loop
                    return self.get_richter_class(update_if_fails=False)
                else:
                    warnings.warn("You don't have the permissions to change something on the database.\nTherefor an update of the richter_class is not possible.")
                    return None
            else:
                return None
        else:
            return res[0]

    def get_horizon(self):
        """Get the value for the horizon angle. (Horizontabschirmung)

        This value is defined by Richter (1995) as the mean horizon angle in the west direction as:
        H’=0,15H(S-SW) +0,35H(SW-W) +0,35H(W-NW) +0, 15H(NW-N)

        Returns
        -------
        float or None
            The mean western horizon angle
        """
        return self.get_meta(infos="horizon")


class StationND(StationNBase, StationCanVirtualBase):
    """A class to work with and download daily precipitation data for one station.

    Those station data are only downloaded to do some quality checks on the 10 minute data.
    Therefor there is no special quality check and richter correction done on this data.
    If you want daily precipitation data, better use the 10 minutes station(StationN) and aggregate to daily values."""
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/",
        "climate_environment/CDC/observations_germany/climate/daily/more_precip/"]
    _para = "n_d"
    _para_long = "daily Precipitation"
    _cdc_col_names_imp = ["RSK"]
    _db_col_names_imp = ["raw"]
    _tstp_format_db = "%Y%m%d"
    _tstp_format_human = "%Y-%m-%d"
    _tstp_dtype = "date"
    _interval = "1 day"
    _min_agg_to = "day"
    _unit = "mm/day"
    _valid_kinds = ["raw", "filled", "filled_by"]
    _best_kind = "filled"

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
        df_all = super()._download_raw(zipfiles)

        # fill RSK with values from RS if not given
        if "RS" in df_all.columns and "RSK" in df_all.columns:
            mask = df_all["RSK"].isna()
            df_all.loc[mask, "RSK"] = df_all.loc[mask, "RS"]
        elif "RS" in df_all.columns:
            df_all["RSK"] = df_all["RS"]

        return df_all

    @check_superuser
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
        with DB_ENG.connect() as con:
            con.execute(sql_add_table)


class StationT(StationTETBase):
    """A class to work with and download temperaure data for one station."""
    _ftp_folder_base = [
        "climate_environment/CDC/observations_germany/climate/daily/kl/"]
    _date_col = "MESS_DATUM"
    _para = "t"
    _para_long = "Temperature"
    _cdc_col_names_imp = ["TMK"]
    _unit = "°C"
    _decimals = 10
    _ma_cols = ["t_year"]
    _coef_sign = ["-", "+"]
    _agg_fun = "avg"

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = dwd_id_to_str(id)

    def _get_sql_new_qc(self, period):
        # create sql for new qc
        sql_new_qc = """
            WITH nears AS ({sql_near_mean})
            SELECT
                timestamp,
                (CASE WHEN (ABS(nears.raw - nears.mean) > 5)
                    THEN NULL
                    ELSE nears."raw" END) as qc
            FROM nears
        """ .format(
            sql_near_mean=self._get_sql_near_mean(period=period, only_real=True))

        return sql_new_qc

    def get_multi_annual(self):
        mas = super().get_multi_annual()
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


class StationET(StationTETBase):
    """A class to work with and download potential Evapotranspiration (VPGB) data for one station."""
    _ftp_folder_base = ["climate_environment/CDC/derived_germany/soil/daily/"]
    _date_col = "Datum"
    _para = "et"
    _para_long = "Evapotranspiration"
    _cdc_col_names_imp = ["VPGB"]
    _unit = "mm/Tag"
    _decimals = 10
    _ma_cols = ["et_year"]
    _sql_add_coef_calc = "* ma.exp_fact::float/ma_stat.exp_fact::float"

    def __init__(self, id, **kwargs):
        super().__init__(id, **kwargs)
        self.id_str = str(id)

    def _get_sql_new_qc(self, period):
        # create sql for new qc
        sql_new_qc = """
            WITH nears AS ({sql_near_mean})
            SELECT
                timestamp,
                (CASE WHEN ((nears.raw > (nears.mean * 2) AND nears.raw > 3)
                            OR (nears.raw < (nears.mean * 0.5) AND nears.raw > 2))
                    THEN NULL
                    ELSE nears."raw" END) as qc
            FROM nears
        """ .format(
            sql_near_mean=self._get_sql_near_mean(
                period=period, only_real=True)
        )

        return sql_new_qc

    def get_adj(self, **kwargs):
        main_df, adj_df, ma, main_df_tr = super().get_adj(**kwargs)

        # calculate the yearly
        main_df_y = main_df.groupby(main_df_tr.index.year)\
            .sum(min_count=345).mean()

        adj_df["adj"] = (main_df * (ma[0] / main_df_y)).round(1)

        return adj_df


# create a grouping class for the 3 parameters together
class GroupStation(object):
    """A class to group all possible parameters of one station.

    So if you want to create the input files for a simulation, where you need T, ET and N, use this class to download the data for one station.
    """

    def __init__(self, id, error_if_missing=True, **kwargs):
        self.id = id
        self.station_parts = []
        self._error_if_missing = error_if_missing
        for StatClass in [StationN, StationT, StationET]:
            try:
                self.station_parts.append(
                    StatClass(id=id, **kwargs)
                )
            except Exception as e:
                if error_if_missing:
                    raise e
        self.paras_available = [stat._para for stat in self.station_parts]

    def _check_paras(self, paras):
        if type(paras)==str and paras != "all":
            paras = [paras,]

        if paras == "all":
            return self.paras_available
        else:
            paras_new = []
            for para in paras:
                if para in self.paras_available:
                    paras_new.append(para)
                elif self._error_if_missing:
                    raise ValueError(
                        f"The parameter {para} you asked for is not available for station {self.id}")
            return paras_new

    @staticmethod
    def _check_kinds(kinds):
        # type cast kinds
        if type(kinds) == str:
            kinds = [kinds]
        else:
            kinds = kinds.copy()
        return kinds

    def get_available_paras(self, short=False):
        """Get the possible parameters for this station.

        Parameters
        ----------
        short : bool, optional
            Should the short name of the parameters be returned.
            The default is "long".

        Returns
        -------
        list of str
            A list of the long parameter names that are possible for this station to get.
        """
        paras = []
        attr_name = "_para" if short else "_para_long"
        for stat in self.station_parts:
            paras.append(getattr(stat, attr_name))

        return paras

    def get_filled_period(self, kinds="best", from_meta=True, join_how="inner"):
        """Get the combined filled period for all 3 stations.

        This is the maximum possible timerange for these stations.

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
            The default is True.
        join_how : str, optional
            How should the different periods get joined.
            If "inner" then the minimal period that is inside of all the filled_periods is returned.
            If "outer" then the maximal possible period is returned.
            The default is "inner".

        Returns
        -------
        TimestampPeriod
            The maximum filled period for the 3 parameters for this station.
        """
        kinds = self._check_kinds(kinds)
        for kind in ["filled_by", "adj"]:
            if kind in kinds:
                kinds.remove(kind)

        # get filled_period
        for kind in kinds:
            for stat in self.station_parts:
                new_filled_period =  stat.get_filled_period(
                    kind=kind, from_meta=from_meta)

                if "filled_period" not in locals():
                    filled_period = new_filled_period.copy()
                else:
                    filled_period = filled_period.union(
                        new_filled_period, how=join_how)

        return filled_period

    def get_df(self, period=(None, None), kinds="best", paras="all",
               agg_to="day", nas_allowed=True, add_na_share=False):
        """Get a DataFrame with the corresponding data.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kinds :  str or list of str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by", "best"("corr" for N and "filled" for T and ET).
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is True.
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
        add_na_share : bool, optional
            Should one or several columns be added to the Dataframe with the share of NAs in the data.
            This is especially important, when the stations data get aggregated, because the aggregation doesn't make sense if there are a lot of NAs in the original data.
            If True, one column per asked kind is added with the respective share of NAs, if the aggregation step is not the smallest.
            The "kind"_na_share column is in percentage.
            The default is False.

        Returns
        -------
        pd.Dataframe
            A DataFrame with the timeseries for this station and the given period.
        """
        paras = self._check_paras(paras)

        # download dataframes
        dfs = []
        for stat in self.station_parts:
            if stat._para in paras:
                df = stat.get_df(
                    period=period,
                    kinds=kinds,
                    agg_to=agg_to,
                    nas_allowed=nas_allowed,
                    add_na_share=add_na_share)
                df = df.rename(dict(zip(
                    df.columns,
                    [stat._para.upper() + "_" + col for col in df.columns])),
                    axis=1)
                dfs.append(df)

        # concat the dfs
        if len(dfs) > 1:
            df_all = pd.concat(dfs, axis=1)
        elif len(dfs) == 1 :
            df_all = dfs[0]
        else:
            raise ValueError("No timeserie was found for {paras} and Station {stid}".format(
                paras=", ".join(paras) is type(paras),
                stid=self.id))

        return df_all

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
        return StationBase.get_meta_explanation(infos=infos)

    def get_max_period(self, kinds, nas_allowed=False):
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
        utils.TimestampPeriod
            The maximum Timestamp Period
        """
        kinds = self._check_kinds(kinds)
        for stat in self.station_parts:
            max_period_i = stat.get_max_period(
                kinds=kinds, nas_allowed=nas_allowed)
            if "max_period" in locals():
                max_period = max_period.union(
                    max_period_i,
                    how="outer" if nas_allowed else "inner")
            else:
                max_period = max_period_i

        return max_period

    def get_meta(self, paras="all", **kwargs):
        """Get the meta information for every parameter of this station.

        Parameters
        ----------
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
        kwargs : dict, optional
            The optional keyword arguments are handed to the single Station get_meta methodes. Can be e.g. "info".

        Returns
        -------
        dict
            dict with the information.
            there is one subdict per parameter.
            If only one parameter is asked for, then there is no subdict, but only a single value.
        """
        paras = self._check_paras(paras)

        for stat in self.station_parts:
            if stat._para in paras:
                meta_para = stat.get_meta(**kwargs)
                if "meta_all" not in locals():
                    meta_all = {stat._para:meta_para}
                else:
                    meta_all.update({stat._para:meta_para})
        return meta_all

    def get_geom(self):
        return self.station_parts[0].get_geom()

    def get_name(self):
        return self.station_parts[0].get_name()

    def create_roger_ts(self, dir, period=(None, None),
                        kind="best", r_r0=1):
        """Create the timeserie files for roger as csv.

        This is only a wrapper function for create_ts with some standard settings.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        r_r0 : int or float, list of int or float or None, optional
            Should the ET timeserie contain a column with R/R0.
            If None, then no column is added.
            If int or float, then a R/R0 column is appended with this number as standard value.
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appanded to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The serie is then joined to the ET timeserie.
            The default is 1.

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        return self.create_ts(dir=dir, period=period, kinds=kind,
                              agg_to="10 min", r_r0=r_r0, split_date=True,
                              nas_allowed=False)

    def create_ts(self, dir, period=(None, None), kinds="best", paras="all",
                  agg_to="10 min", r_r0=None, split_date=False,
                  nas_allowed=True, add_na_share=False):
        """Create the timeserie files as csv.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kinds :  str or list of str
            The data kinds to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by", "best".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
            If only one kind is asked for, then the columns get renamed to only have the parameter name as column name.
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".
        r_r0 : int or float or None or pd.Series or list, optional
            Should the ET timeserie contain a column with R/R0.
            If None, then no column is added.
            If int, then a R/R0 column is appended with this number as standard value.
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appanded to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The serie is then joined to the ET timeserie.
            The default is None.
        split_date : bool, optional
            Should the timestamp get splitted into parts, so one column for year, one for month etc.?
            If False the timestamp is saved in one column as string.
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

        Raises
        ------
        Warning
            If there are NAs in the timeseries and nas_allowed is False
            or the period got changed.
        """
        # check directory
        dir = self._check_dir(dir)

        # type cast kinds
        kinds = self._check_kinds(kinds)
        paras = self._check_paras(paras)

        # get the period
        join_how = "outer" if nas_allowed else "inner"

        period = TimestampPeriod._check_period(period)
        period_filled = self.get_filled_period(kinds=kinds, join_how=join_how)

        if period.is_empty():
            period = period_filled
        else:
            period_new = period_filled.union(
                period,
                how="inner")
            if period_new != period:
                warnings.warn(
                    "The Period for Station {stid} got changed from {period} to {period_filled}.".format(
                        stid=self.id,
                        period=str(period),
                        period_filled=str(period_filled)))
                period = period_new

        # prepare loop
        name_suffix = "_{stid:0>4}.txt".format(stid=self.id)
        x, y = self.get_geom().split(";")[1]\
                .replace("POINT(", "").replace(")", "")\
                .split(" ")
        name = self.get_name() + " (ID: {stid})".format(stid=self.id)
        do_zip = type(dir) == zipfile.ZipFile

        for para in paras:
            # get the timeserie
            df = self.get_df(
                period=period, kinds=kinds,
                paras=[para], agg_to=agg_to,
                nas_allowed=nas_allowed,
                add_na_share=add_na_share)

            # rename columns
            if len(kinds)==1:
                df.rename(
                    {df.columns[0]: para.upper()},
                    axis=1, inplace=True)
            elif "filled_by" in kinds and len(kinds)==2:
                df.rename(
                    {f"{para.upper()}_" + kinds[1-(kinds.index("filled_by"))]: para.upper()},
                    axis=1, inplace=True)
            else:
                df.rename(
                    dict(zip(df.columns,
                            [col.replace(f"{para}_", f"{para.upper()}_")
                            for col in df.columns])),
                    axis=1, inplace=True)

            # check for NAs
            filled_cols = [col for col in df.columns if "filled_by" in col]
            if not nas_allowed and df.drop(filled_cols, axis=1).isna().sum().sum() > 0:
                warnings.warn("There were NAs in the timeserie for Station {stid}.".format(
                    stid=self.id))

            # get the number of columns
            num_col = 1
            if split_date:
                num_col += AGG_TO[agg_to]["split"][para]
            else:
                num_col += 1

            # special operations for et
            if para == "et" and r_r0 is not None:
                num_col += 1
                if type(r_r0)==int or type(r_r0)==float:
                    df = df.join(
                        pd.Series([r_r0]*len(df), name="R/R0", index=df.index))
                elif type(r_r0)==pd.Series:
                    df = df.join(r_r0.rename("R_R0"))
                elif type(r_r0)==list:
                    df = df.join(
                        pd.Series(r_r0, name="R/R0", index=df.index))

            # create header
            header = ("Name: " + name + "\t" * (num_col-1) + "\n" +
                        "Lat: " + y + "   ,Lon: " + x + "\t" * (num_col-1) + "\n")

            # create tables
            if split_date:
                df = self._split_date(df.index)\
                        .iloc[:, 0:AGG_TO[agg_to]["split"][para]]\
                        .join(df)
            else:
                df.reset_index(inplace=True)

            # write table out
            str_df = header + df.to_csv(
                sep="\t", decimal=".", index=False, line_terminator="\n")
            file_name = para.upper() + name_suffix
            if do_zip:
                dir.writestr(
                    "{stid}/{file}".format(
                        stid=self.id, file=file_name),
                    str_df)
            else:
                with open(dir.joinpath(file_name), "w") as f:
                    f.write(str_df)

    @staticmethod
    def _check_dir(dir):
        """Checks if a directors is valid and empty.

        If not existing the directory is created.

        Parameters
        ----------
        dir : pathlib object or zipfile.ZipFile
            The directory to check.

        Raises
        ------
        ValueError
            If the directory is not empty.
        ValueError
            If the directory is not valid. E.G. it is a file path.
        """
        # check types
        if type(dir) == str:
            dir = Path(dir)

        # check directory
        if isinstance(dir, Path):
            if dir.is_dir():
                if len(list(dir.iterdir())) > 0:
                    raise ValueError(
                        "The given directory '{dir}' is not empty.".format(
                            dir=str(dir)))
            elif dir.suffix == "":
                dir.mkdir()
            else:
                raise ValueError(
                    "The given directory '{dir}' is not a directory.".format(
                        dir=dir))
        elif not isinstance(dir, zipfile.ZipFile):
            raise ValueError(
                "The given directory '{dir}' is not a directory or zipfile.".format(
                    dir=dir))

        return dir

    @staticmethod
    def _split_date(dates):
        """
        Split datetime into parts.

        Parameters
        ----------
        dates : pandas.DatetimeIndex or list of (datetime.dateime or pandas.Timestamp) or
                pandas.DataFrame of (datetime.datetime or pandas.Timestamp)
            The datetime's to split.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with 5 columns (Jahr, Monat, Tag, Stunde, Minute).
        """
        # if dates is not a list make it a list
        if type(dates) == datetime or type(dates) == pd.Timestamp:
            dates = pd.DatetimeIndex([dates])
            index = range(0, len(dates))

        elif type(dates) == pd.DatetimeIndex:
            index = dates
        else:
            index = range(0, len(dates))

        # check if date is datetime or Timestamp:
        if not (type(dates[0]) == pd.Timestamp or
                type(dates[0]) == datetime):
            raise ValueError("Error: The given date is not in a datetime or " +
                            "Timestamp format.")

        return pd.DataFrame(
            {"Jahr": dates.year,
             "Monat": dates.month,
             "Tag": dates.day,
             "Stunde": dates.hour,
             "Minute": dates.minute},
             dtype=int,
             index=index)
