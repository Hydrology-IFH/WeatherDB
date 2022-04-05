

# libraries
import itertools
import logging
import re
import time
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from sqlalchemy.exc import OperationalError

import shapely.wkt

from ..lib.connections import CDC, DB_ENG, check_superuser
from ..lib.max_fun.import_DWD import get_dwd_file
from ..lib.utils import TimestampPeriod, get_ftp_file_list

# Variables
MIN_TSTP = datetime.strptime("19940101", "%Y%m%d")
THIS_DIR = Path(__file__).parent.resolve()
DATA_DIR = THIS_DIR.parents[2].joinpath("data")
RASTERS = {
    "dwd_grid": {
        "srid": 31467,
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
    _kinds_not_decimal = ["qn", "filled_by"]
    _tstp_format = None  # The format string for the strftime
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

    def __init__(self, id):
        if type(self) == StationBase:
            raise NotImplementedError("""
            The StationBase is only a wrapper class an is not working on its own.
            Please use PrecipitationStation, TemperatureStation or EvapotranspirationStation instead""")
        self.id = id
        self.id_str = str(id)

        if type(self._ftp_folder_base) == str:
            self._ftp_folder_base = [self._ftp_folder_base]

        # create ftp_folders in order of importance
        self._ftp_folders = list(itertools.chain(*[
            [base + "historical/", base + "recent/"]
            for base in self._ftp_folder_base]))

        self._db_unit = " ".join([str(self._decimals), self._unit])
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
        """Check if the kind has a timestamp von and bis in the meta table."""
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
        for i, kind_i in enumerate(kinds):
            if kind_i not in self._valid_kinds:
                kinds[i] = self._check_kind(kind_i)
        return kinds

    def _check_period(self, period, kinds):
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

        Returns
        -------
        list with 2 datetime.datetime
            The minimum and maximum Timestamp.
        """
        # check if period gor recently checked
        self._clean_cached_period()
        cache_key = str((kinds, period))
        if cache_key in self._cached_periods:
            return self._cached_periods[cache_key]["return"]

        # get filled period
        filled_period = self.get_filled_period(kind=kinds[0])
        for kind in kinds[1:]:
            filled_period = filled_period.union(
                self.get_filled_period(kind=kind),
                how="outer")

        if filled_period.has_only_NaT():
            raise ValueError(
                "No filled period was found for {para_long} Station with ID {stid} and kinds '{kinds}'."
                .format(
                    para_long=self._para_long, stid=self.id, kinds="', '".join(kinds)))

        # get period if None providen and compare with filled_period
        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        if period.is_empty():
            period = filled_period
        else:
            period = period.union(filled_period, how="inner")

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

    def _check_df_raw(self, df_raw):
        """This is a empty function to get implemented in the subclasses if necessary.
        
        It applies extra checkups on the downloaded raw timeserie and returns the dataframe."""
        return df_raw

    def _clean_cached_period(self):
        time_limit = datetime.now() - timedelta(minutes=1)
        for key in list(self._cached_periods):
            if self._cached_periods[key]["time"] < time_limit:
                self._cached_periods.pop(key)

    @check_superuser
    def _check_ma(self):
        if not self.isin_ma():
            self._update_ma()

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
            select {stid} in (select station_id from meta_{para});
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
        return not self.is_virtual()

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
            SET {kind}_von={min_tstp},
                {kind}_bis={max_tstp}
            WHERE station_id={stid};
        """.format(
            stid=self.id, para=self._para,
            kind=kind,
            **period.get_sql_format_dict(
                format="'{}'".format(self._tstp_format))
        )

        with DB_ENG.connect() as con:
            con.execute(sql)

    @check_superuser
    def update_ma(self, skip_if_exist=True):
        """Update the multi annual values in the stations_raster_values table.

        Get new values from the raster and put in the table.
        """
        if self.isin_ma() and skip_if_exist:
            return None

        sql_new_mas = """
            WITH stat_geom AS (
                SELECT ST_TRANSFORM(geometry, {raster_srid}) AS geom
                FROM meta_{para}
                WHERE station_id={stid}
            )
            SELECT {calc_line}
            FROM rasters.{raster_name};
        """.format(
            stid=self.id, para=self._para,
            raster_name=self._ma_raster["db_table"],
            raster_srid=self._ma_raster["srid"],
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
                (station_id, raw_von, raw_bis, last_imp_von, last_imp_bis
                    {last_imp_cols})
            VALUES ({stid}, {min_tstp}, {max_tstp}, {min_tstp},
                    {max_tstp}{last_imp_values})
            ON CONFLICT (station_id) DO UPDATE SET
                raw_von = LEAST (meta.raw_von, EXCLUDED.raw_von),
                raw_bis = GREATEST (meta.raw_bis, EXCLUDED.raw_bis),
                last_imp_von = CASE WHEN {last_imp_test}
                                    THEN EXCLUDED.last_imp_von
                                    ELSE LEAST(meta.last_imp_von,
                                            EXCLUDED.last_imp_von)
                                    END,
                last_imp_bis = CASE WHEN {last_imp_test}
                                    THEN EXCLUDED.last_imp_bis
                                    ELSE GREATEST(meta.last_imp_bis,
                                                EXCLUDED.last_imp_bis)
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
                format="'{}'".format(self._tstp_format)))

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
                    **imp_period.get_sql_format_dict(format="%Y-%m-%d %H:%M")))

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
        for zipfile in zipfiles:
            if "df_all" not in locals():
                df_all = get_dwd_file(zipfile)
            else:
                df_new = get_dwd_file(zipfile)
                # cut out if already in previous file
                df_new = df_new[~df_new[self._date_col].isin(
                    df_all[self._date_col])]
                # concatenat the dfs
                df_all = pd.concat([df_all, df_new])
                df_all.reset_index(drop=True, inplace=True)

        # check for duplicates in date column
        df_all.set_index(self._date_col, inplace=True)
        if df_all.index.has_duplicates:
            df_all = df_all.groupby(df_all.index).mean()

        # run extra checks of subclasses
        df_all = self._check_df_raw(df_all)

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
                    format=self._tstp_format)
                ))

        # mark last import as done if in period
        last_imp_period = self.get_last_imp_period()
        if last_imp_period.inside(period):
            self._mark_last_imp_done(kind="qc")

    @check_superuser
    def fillup(self, period=(None, None), **kwargs):
        """Fill up missing data with measurements from nearby stations."""
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
                        format="'{}'".format(self._tstp_format)))
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
                            meta.raw_von, meta.raw_bis,
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
                        CONTINUE WHEN i.raw_von > unfilled_period.max
                                      OR i.raw_bis < unfilled_period.min;
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
                **period.get_sql_format_dict()))

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
            self._mark_last_imp_done(kind="qc")

    @check_superuser
    def last_imp_qc(self):
        self.last_imp_quality_check()

    @check_superuser
    def last_imp_fillup(self, _last_imp_period=None):
        """Do the filling up of the last import.
        """
        if not self.is_last_imp_done(kind="filled"):
            if _last_imp_period is None:
                period = self.get_last_imp_period(all=True)
            else:
                period = _last_imp_period

            self.fillup(period=period)
            self._mark_last_imp_done(kind="filled")

    def get_meta(self, infos="all"):
        """Get Informations from the meta table.

        Parameters
        ----------
        infos : list of str or str, optional
            A list of the informations to get from the database.
            If "all" then all the informations are returned.
            The default is "all".

        Returns
        -------
        list
            list with the informations.
        """
        # check which informations to get
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

        return res.first()[0]

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
                SELECT min({kind}_von) as {kind}_von,
                    max({kind}_bis) as {kind}_bis
                FROM meta_{para};
            """.format(**sql_format_dict)
        else:
            sql = """
                SELECT {kind}_von, {kind}_bis
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

    def get_df(self, kinds, period=(None, None), agg_to=None, db_unit=False):
        """Get a timeseries DataFrame from the database.

        Parameters
        ----------
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
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
            adj_df = self.get_adj(period=period)
            if len(kinds) == 1:
                return adj_df
            else:
                kinds.remove("adj")

        # check kinds and period
        kinds = self._check_kinds(kinds=kinds)
        period = self._check_period(period=period, kinds=kinds)

        if period.is_empty():
            return None

        # aggregating?
        timestamp_col = "timestamp"
        group_by = ""
        agg_to = self._check_agg_to(agg_to)
        if agg_to is not None:
            # create sql parts
            kinds = [
                "{agg_fun}({kind}) AS {kind}".format(
                    agg_fun=self._agg_fun, kind=kind) for kind in kinds]
            timestamp_col = "date_trunc('{agg_to}', timestamp)".format(
                agg_to=agg_to)
            group_by = "GROUP BY " + timestamp_col

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
                format="'{}'".format(self._tstp_format))
        )

        df = pd.read_sql(sql, con=DB_ENG, index_col="timestamp")

        # change index to pandas DatetimeIndex if necessary
        if type(df.index) != pd.DatetimeIndex:
            df.set_index(pd.DatetimeIndex(df.index), inplace=True)

        # change to normal unit
        if not db_unit:
            change_cols = [
                col for col in df.columns
                if col not in self._kinds_not_decimal]
            df[change_cols] = df[change_cols] / self._decimals

        # check if adj should be added:
        if "adj_df" in locals():
            df = df.join(adj_df)

        return df

    def get_raw(self, period=(None, None), agg_to=None):
        """Get the raw timeserie.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeserie.
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

        Returns
        -------
        pd.DataFrame
            The raw timeserie for this station and the given period.
        """
        return self.get_df(period=period, kinds="raw", agg_to=agg_to)

    def get_qc(self, period=(None, None), agg_to=None):
        """Get the quality checked timeserie.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeserie.
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

        Returns
        -------
        pd.DataFrame
            The quality checked timeserie for this station and the given period.
        """
        return self.get_df(period=period, kinds="qc", agg_to=agg_to)

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
                format="'{}'".format(self._tstp_format)
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
            df = df.join(self.get_dist())

        return df

    def get_adj(self, period=(None, None), agg_to=None):
        """Get the adjusted timeserie.

        The timeserie is adjusted to the multi annual mean.
        So the overall mean of the given period will be the same as the multi annual mean.

        Parameters
        ----------
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

        Returns
        -------
        pandas.DataFrame
            A timeserie with the adjusted data.
        """
        # this is only the first part of the methode
        # get basic values
        main_df = self.get_df(
            period=period,
            kinds=[self._best_kind],
            agg_to=agg_to)
        ma = self.get_multi_annual()

        # create empty adj_df
        adj_df = pd.DataFrame(
            columns=["adj"],
            index=main_df.index,
            dtype=main_df[self._best_kind].dtype)

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
    But to have data for every parameter at every 10 min precipitation station location, it is necessary to add stations and fill them with data from neighboors."""

    def _check_isin_meta(self):
        """Check if the Station is in the Meta table and if not create a virtual station.

        Raises:
            NotImplementedError:
                If the Station ID is neither a real station or in the precipitation meta table.

        Returns:
            bool: True if the Station check was successfull.
        """
        if self.isin_meta():
            return True
        elif self.isin_meta_n():
            self._create_meta_virtual()
            self._create_timeseries_table()
            return True
        else:
            raise NotImplementedError("""
                The given {para_long} station with id {stid}
                is not in the corresponding meta table
                and not in the precipitation meta table in the DB""".format(
                stid=self.id, para_long=self._para_long
            ))

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
    _tstp_format = "%Y%m%d"

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

    def get_adj(self, period=(None, None)):
        # this is only the second part of the methode
        main_df, adj_df, ma = super().get_adj(period=period)

        # truncate to full years
        tstp_min = main_df.index.min()
        if tstp_min > pd.Timestamp(year=tstp_min.year, month=1, day=15):
            tstp_min = pd.Timestamp(
                year=tstp_min.year+1, month=1, day=1)

        tstp_max = main_df.index.max()
        if tstp_max < pd.Timestamp(year=tstp_min.year, month=12, day=15):
            tstp_min = pd.Timestamp(
                year=tstp_min.year-1, month=12, day=31)

        main_df_tr = main_df.truncate(tstp_min, tstp_max)

        # the rest must get implemented in the subclasses
        return main_df, adj_df, ma, main_df_tr


class StationNBase(StationBase):
    _date_col = "MESS_DATUM"
    _decimals = 100
    _ma_cols = ["n_regnie_wihj", "n_regnie_sohj"]
    _ma_raster = RASTERS["regnie_grid"]

    def get_adj(self, period=(None, None)):
        main_df, adj_df, ma = super().get_adj(period=period)

        # calculate the half yearly mean
        # sohj
        sohj_months = [4, 5, 6, 7, 8, 9]
        mask_sohj = main_df.index.month.isin(sohj_months)

        main_df_sohj = main_df[mask_sohj]

        main_df_sohj_y = main_df_sohj.groupby(main_df_sohj.index.year)\
            .sum(min_count=(183 - 10) * 24 * 6).mean()

        adj_df[mask_sohj] = (main_df_sohj * (ma[1] / main_df_sohj_y)).round(2)

        # wihj
        mask_wihj = ~mask_sohj
        main_df_wihj = main_df[mask_wihj]
        main_df_wihj_y = main_df_wihj.groupby(main_df_wihj.index.year)\
            .sum(min_count=(183 - 10) * 24 * 6).mean()
        adj_df[mask_wihj] = (main_df_wihj * (ma[0] / main_df_wihj_y)).round(2)

        return adj_df

