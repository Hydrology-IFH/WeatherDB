import unittest
import sys
import os
import sqlalchemy as sa
import argparse
from pathlib import Path
import pandas as pd
from distutils.util import strtobool
from datetime import timezone
import shutil
from tempfile import TemporaryDirectory

from weatherdb.db import models
import weatherdb as wdb

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# get cli variables
parser = argparse.ArgumentParser(description="InitDB Test CLI arguments")
parser.add_argument("--steps", default="all",
                    help="The steps to run. Default is 'all'.")
parser.add_argument("--just-check", action="store_true",
                    help="Should only the check be run, without the weatherDB step method.")
cliargs, remaining_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + remaining_args

# define TestCases class
class InitDBTestCases(BaseTestCases):

    @classmethod
    def setUpClass(cls):
        if cliargs.steps == "all":
            cls.log.debug("Recreating database...")
            cls.empty_db()
            cls.broker.create_db_schema(if_exists="DROP", silent=True)
        else:
            cls.broker.create_db_schema(if_exists="IGNORE", silent=True)
            cls.log.debug("Working with previous database state.")

        # setup results list
        cls._test_results = []

    @classmethod
    def tearDownClass(cls):
        # check if errors occurred
        art_dir = Path(
            os.environ.get("WEATHERDB_TEST_ARTIFACT_DIR",
                           Path(__file__).parent.joinpath("artifacts")))
        is_failure = any([len(res.failures) > 0 for res in cls._test_results])

        def check_flag(key, default="false"):
            flag_val = os.environ.get(key, default).lower()
            if flag_val == "on_failure":
                return is_failure
            return strtobool(flag_val)

        # save artifacts if asked for
        if check_flag("WEATHERDB_TEST_ARTIFACT_DB_DUMP"):
            dump_dir = art_dir.joinpath("db_dump")
            cls.log.debug(f"Saving artifacts to {dump_dir}")

            # get database content
            metadata = wdb.db.models.ModelBase.metadata
            metadata.reflect(bind=cls.db_engine.engine, schema="timeseries")
            with cls.db_engine.engine.connect() as conn:
                db = {"public": {}, "timeseries": {}}
                for table in metadata.sorted_tables:
                    db[table.schema][table.name] = pd.read_sql_table(
                        table.name,
                        conn,
                        schema=table.schema)

            # dump dict to csv files
            for schema, tables in db.items():
                schema_dir = dump_dir.joinpath(schema)
                schema_dir.mkdir(parents=True, exist_ok=True)
                for table_name, df in tables.items():
                    df.to_csv(
                        schema_dir.joinpath(f"{table_name}.csv"),
                        index=False)

        # list files in data folder
        if check_flag("WEATHERDB_TEST_ARTIFACT_LIST_DATA_FILES"):
            cls.log.debug("Listing files in data folder")
            with open(art_dir.joinpath("data_files.csv"), "w") as f:
                f.write("Path;Size in MB\n")
                for path in Path(wdb.config.get("data", "base_dir")).glob("**/*"):
                    f.write(f"{path};{os.path.getsize(path)/(1024^2)}\n")

        # copy user config file
        if check_flag("WEATHERDB_TEST_ARTIFACT_COPY_USER_CONFIG"):
            shutil.copy(
                wdb.config.user_config_file,
                art_dir.joinpath("user_config.ini").as_posix())

    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
            result.failfast = True
        self._resultForDoCleanups = result
        self.test_result = result  # Store the result object
        self._test_results.append(result)
        super().run(result)

    # steps for initiating the database
    def step_update_meta(self, **kwargs):
        self.broker.update_meta(**kwargs)

    def step_update_raw(self, **kwargs):
        self.broker.update_raw(only_new=False, **kwargs)

    def step_update_ma_raster(self, **kwargs):
        self.broker.update_ma_raster(**kwargs)

    def step_update_richter_class(self, **kwargs):
        self.broker.stations_p.update_richter_class(
            skip_if_exist=False, **kwargs)

    def step_quality_check(self, **kwargs):
        self.broker.quality_check(**kwargs)

    def step_fillup(self, **kwargs):
        self.broker.fillup(**kwargs)

    def step_richter_correct(self, **kwargs):
        self.broker.richter_correct(**kwargs)

    def step_vacuum(self, **kwargs):
        self.broker.vacuum(**kwargs)

    def step_create_ts(self, **kwargs):
        with TemporaryDirectory() as td:
            zip_fp = Path(td).joinpath("test.zip")
            wdb.GroupStations().create_ts(
                stids=self.test_stids,
                period=("2010-01-01", "2011-01-02"),
                kind="best",
                agg_to="day",
                dir=zip_fp)
            self.assertTrue(
                zip_fp.exists(),
                msg="Zip file was not created.")

    def _check_no_nas(self, base_kind, kind, stats, add_base_stat_class=None, add_base_kind=None):
        """Check if there are NAs in the timeseries, but exclude rows where the base_kind is NULL.

        As there are not enough stations in the test_stids to completly fill the timeseries, the check is done only on the stations where the base kind is not NULL.

        Parameters
        ----------
        base_kind : str
            The base kind to check for NAs to exclude the timestamps from the check
        kind : str
            The current kind to check for NAs
        stats : list of wdb.StationBase-types
            The stations to check for NAs in the kind timeseries.
        add_base_stats : list of wdb.StationBase-type, optional
            An additional station to check for NULL in the base kind timeseries to exclude the timestamps from the check.
            This is especially relevant for richter Corrected data, where the temperature data is needed to create the precipitation data.
        add_base_kind : str, optional
            The base_kind of the additional stat to check for NAs to exclude the timestamps from the check.
        """
        add_base_stats = [add_base_stat_class(stat.id) if add_base_stat_class is not None else None
                          for stat in stats]

        # get all timestamps where no station has qc data, to exclude them from the check
        stmnt_valid_tstps = None
        for stat, add_base_stat in zip(stats, add_base_stats):
            sq = sa.select(stat._table.columns.timestamp)\
                .select_from(stat._table)
            if add_base_stat is not None and add_base_kind is not None:
                sq = sq\
                    .outerjoin(
                        add_base_stat._table,
                        add_base_stat._table.columns.timestamp == stat._table.columns.timestamp.cast(
                            add_base_stat._table.columns.timestamp.type),
                        full=True)\
                    .where(sa.and_(
                        stat._table.columns[add_base_kind] != None,  # noqa: E711
                        add_base_stat._table.columns[add_base_kind] != None))  # noqa: E711
            else:
                sq = sq.where(stat._table.columns[base_kind] != None) # noqa: E711

            if stmnt_valid_tstps is None:
                stmnt_valid_tstps = sq
            else:
                sq = sq.subquery()
                stmnt_valid_tstps = stmnt_valid_tstps.outerjoin(
                    sq,
                    sq.columns.timestamp == stats[0]._table.columns.timestamp,
                    full=True)

        # check for NAs in filled data where at least one qc value is available
        stmnt_valid_tstps_cte = stmnt_valid_tstps.cte("stmnt_valid_tstps")
        stmnts = []
        for stat in stats:
            stmnts.append(
                sa.select(
                    sa.text(f"{stat.id} as station_id"),
                    sa.func.count(stat._table.columns.timestamp).label("count_nas"))\
                .select_from(stat._table)\
                .join(stmnt_valid_tstps_cte,
                      stmnt_valid_tstps_cte.columns.timestamp == stat._table.columns.timestamp)\
                .where(stat._table.columns[kind] == None)) # noqa: E711
        stmnt_all = sa.union(*stmnts)

        # run query
        with self.db_engine.connect() as conn:
            res = conn.execute(stmnt_all).fetchall()

        # check for NAs in query result
        for row in res:
            with self.subTest(stid=row[0]):
                self.assertEqual(
                    row[1],
                    0,
                    msg=f"{kind} timeserie has NAs.")

    def _check_vals_where_nas(self, base_kind, kind, stat):
        """Check if there are values in kind where the base_kind doesn't have values.

        Parameters
        ----------
        base_kind : str
            The base kind to check for where NAs should be.
        kind : str
            The current kind to check for wrong values.
        stat : wdb.StationBase-type
            The station to check for NAs in the kind timeseries.
        """
        # check for values where filled has no values
        stmnt = sa.select(sa.func.count(stat._table.columns.timestamp))\
            .select_from(stat._table)\
            .where(sa.and_(stat._table.columns[base_kind] == None,  # noqa: E711
                            stat._table.columns[kind] != None))  # noqa: E711
        with self.db_engine.connect() as conn:
            self.assertEqual(
                conn.execute(stmnt).scalar(),
                0,
                msg="{kind} timeserie has values where {base_kind} does not.")

    def _check_not_empty(self, kind, stat):
        with self.db_engine.connect() as conn:
            with self.subTest(stid=stat.id):
                stmnt = sa.select(sa.func.count(stat._table.c[kind]))\
                    .select_from(stat._table)
                self.assertGreater(
                    conn.execute(stmnt).scalar(), 0,
                    msg=f"Table has an empty {kind} column.")

    def check_update_meta(self):
        with self.db_engine.connect() as conn:
            for model, n_expct in zip([models.MetaP, models.MetaT, models.MetaPD, models.MetaET],
                                      [6, 4, 6, 3]):
                # check number of stations
                with self.subTest(model=model):
                    stmnt = sa.select(sa.func.count('*')).select_from(model)
                    n = conn.execute(stmnt).scalar()
                    self.assertGreaterEqual(n, n_expct,
                        msg=f"Number of stations in {model.__name__} table ({n}) is lower than the excpected {n_expct} stations.")
                    self.assertLessEqual(n, len(self.test_stids),
                        msg=f"Number of stations in {model.__name__} table ({n}) is greater than the amount of test stations.")

                # check for last_imp_qc
                if model != models.MetaPD:
                    with self.subTest(model=model):
                        stmnt = sa.select(sa.func.count('*')).select_from(model).where(model.last_imp_qc)
                        self.assertEqual(
                            conn.execute(stmnt).scalar(), 0,
                            msg="Some stations have last_imp_qc=True in meta data."
                        )

                # check for last_imp_fillup
                with self.subTest(model=model):
                    stmnt = sa.select(sa.func.count("*"))\
                        .select_from(model)\
                        .where(model.last_imp_filled)
                    self.assertEqual(
                        conn.execute(stmnt).scalar(), 0,
                        msg="Some stations have last_imp_fillup=True in meta data."
                    )

            # check for last_imp_corr
            with self.subTest(msg="check last_imp_corr for MetaP"):
                stmnt = sa.select(sa.func.count("*"))\
                    .select_from(models.MetaP)\
                    .where(models.MetaP.last_imp_corr)
                self.assertEqual(
                    conn.execute(stmnt).scalar(),
                    0,
                    msg="Some stations have last_imp_corr=True in meta data."
                )

    def check_update_ma_raster(self):
        for stats in self.broker.stations:
            for stat in stats.get_stations(stids=self.test_stids, skip_missing_stids=True):
                mas = stat.get_ma_raster()
                with self.subTest(stat=stat, mas=mas):
                    self.assertTrue(
                        all([ma is not None for ma in mas]),
                        msg=f"Station {stat.id} of {stat._para_long} Station has no multi annual data.")

    def check_update_raw(self):
        for stats in self.broker.stations:
            # check for existing timeseries table
            inspect = sa.inspect(self.db_engine.engine)
            meta = stats.get_meta(stids=self.test_stids)
            for stid in meta.index:
                with self.subTest(stid=stid, para=stats._para):
                    self.assertTrue(
                        inspect.has_table(f"{stid}_{stats._para}",
                                            schema="timeseries"),
                        msg=f"Timeseries table \"{stid}_{stats._para}\" not found in database.")

            with self.subTest(para=stats._para):
                # get raw data
                df_raw = stats.get_df(
                    kinds="raw",
                    stids=self.test_stids,
                    only_real=False,
                    nas_allowed=True,
                    skip_missing_stids=True,
                    agg_to="day")
                df_meta = stats.get_meta(
                    infos="all",
                    stids=self.test_stids,
                    only_real=False)

                # check number of stations in df_raw
                self.assertEqual(
                    len(df_raw.columns),
                    len(df_meta),
                    msg=f"Number of {stats._para_long} stations in raw data does not match number of stations in the meta table.")

                # check for NA values
                virtual_stids = df_meta[~df_meta["is_real"]].index.values
                self.assertLessEqual(
                    df_raw.isna().all().sum(),
                    len(virtual_stids),
                    msg="The raw data is completly NA for some real stations.")

                # check for time range
                self.assertGreaterEqual(
                    df_raw.index.min(),
                    pd.Timestamp("1994-01-01 00:00:00+0000"),
                    msg="The raw data starts before 1994-01-01.")
                self.assertLessEqual(
                    df_raw.index.max(),
                    pd.Timestamp.now("UTC"),
                    msg="The raw data ends after today.")

                # check meta file
                self.assertEqual(
                    df_meta[df_meta["is_real"]]\
                        [["raw_from", "raw_until", "hist_until", "last_imp_from", "last_imp_from"]]\
                        .isna().all().sum(),
                    0,
                    msg="Some real stations didn't get a raw_from, raw_until, hist_until value in the meta data.")

                for stid in df_meta[df_meta["is_real"]].index:
                    with self.subTest(stid=stid):
                        df_no_na = df_raw.loc[~df_raw[stid].isna(), stid]
                        self.assertGreaterEqual(
                            df_no_na.index.min(),
                            df_meta.loc[stid, "raw_from"].floor("D"),
                            msg="The raw data starts before the raw_from date in the meta data.")
                        self.assertLessEqual(
                            df_no_na.index.max().ceil("D"),
                            df_meta.loc[stid, "raw_until"],
                            msg="The raw data ends after the raw_until date in the meta data.")

    def check_update_richter_class(self):
        with self.db_engine.connect() as conn:
            self.assertEqual(
                conn.execute(
                    sa.select(sa.sql.expression.func.count())\
                    .select_from(models.MetaP)\
                    .where(models.MetaP.richter_class.is_(None))
                ).scalar(),
                0,
                msg="Some stations don't have a richter_class in meta data."
            )

    def check_quality_check(self):
        for stats in [self.broker.stations_p, self.broker.stations_t, self.broker.stations_et]:
            for stat in stats.get_stations(stids=self.test_stids, skip_missing_stids=True):
                fperiod_raw = stat.get_filled_period(kind="raw")
                fperiod_qc = stat.get_filled_period(kind="qc")
                with self.subTest(stat=stat.id, para=stat._para):
                    self._check_not_empty("qc", stat)
                    self.assertGreaterEqual(
                        fperiod_raw,
                        fperiod_qc,
                        msg="Quality checked timeserie is larger than raw data.")
                    if not fperiod_raw.is_empty():
                        self.assertFalse(
                            fperiod_qc.is_empty(),
                            msg="Quality checked timeserie is empty.")

                    # check for values where filled has no values
                    self._check_vals_where_nas("raw", "qc", stat)

    def check_fillup(self):
        for statsPara in self.broker.stations:
            stats = statsPara.get_stations(stids=self.test_stids, skip_missing_stids=True)

            # get meta and reference max_period
            base_kind = "raw" if statsPara._para == "p_d" else "qc"
            meta = statsPara.get_meta(infos=[f"{base_kind}_from", f"{base_kind}_until"])
            with self.db_engine.connect() as con:
                max_dt = con.execute(sa.text(f"""
                    SELECT LEAST(
                        date_trunc('day',
                                   min(start_tstp_last_imp) - '9h 30min'::INTERVAL
                                   ) - '10 min'::INTERVAL,
                        min(CASE WHEN parameter='p' THEN max_tstp_last_imp
                                 ELSE max_tstp_last_imp + '23h 50min'::INTERVAL
                            END))::{statsPara._StationClass._tstp_dtype}
                    FROM parameter_variables;""")
                    ).scalar()
                max_dt = pd.Timestamp(max_dt, tz=timezone.utc)
            max_period = wdb.utils.TimestampPeriod(
                meta[f"{base_kind}_from"].min(),
                max_dt).set_tz(timezone.utc)

            # loop over stations to check the periods
            for stat in stats:
                ts_period = stat.get_max_period(kinds="filled", nas_allowed=True).set_tz(timezone.utc)
                ts_period_filled = stat.get_filled_period(kind="filled").set_tz(timezone.utc)
                with self.subTest(stat=stat.id, para=stat._para):
                    self._check_not_empty("filled", stat)

                    self.assertGreaterEqual(
                        ts_period,
                        max_period,
                        msg="Filled timeserie is smaller than maximum available data range.")

                    self.assertFalse(
                        ts_period_filled.is_empty(),
                        msg="Filled timeserie is empty.")

            # check for NAs
            self._check_no_nas(base_kind, "filled", stats)

    def check_richter_correct(self):
        statsPara = self.broker.stations_p
        stats = statsPara.get_stations(stids=self.test_stids, skip_missing_stids=True)

        # get meta and reference max_period
        # meta = statsPara.get_meta(infos=["filled_from", "filled_until"])
        # max_period = wdb.utils.TimestampPeriod(
        #     meta["filled_from"].min(),
        #     meta["filled_until"].max())

        for stat in stats:
            fperiod_corr = stat.get_filled_period(kind="corr")
            with self.subTest(stat=stat.id, para=stat._para):
                self._check_not_empty("corr", stat)
                # self.assertGreaterEqual(
                #     fperiod_corr,
                #     max_period,
                #     msg="Corrected timeserie is smaller than maximum available data range.")

                if not fperiod_corr.is_empty():
                    self.assertFalse(
                        fperiod_corr.is_empty(),
                        msg="Corrected timeserie is empty.")

                # check for values where filled has no values
                self._check_vals_where_nas("filled", "corr", stat)

        # check for NAs
        self._check_no_nas("filled", "corr", stats, wdb.StationT, "filled")

    def check_vacuum(self):
        with self.db_engine.connect() as conn:
            for schema in ["public", "timeseries"]:
                with self.subTest(schema=schema):
                    stmnt = sa.select(sa.func.count("*"))\
                        .select_from(sa.text(
                            f"""pg_stat_user_tables
                                WHERE schemaname = '{schema}'
                                    AND (last_vacuum IS NULL
                                         OR last_vacuum < (now() - interval '20 minutes'))"""))
                    n_vacuumed = conn.execute(stmnt).scalar()
                    self.assertEqual(
                        n_vacuumed,
                        0,
                        msg=f"No tables found in schema {schema}.")

    def check_create_ts(self):
        pass

    def test_steps(self):
        """Test the single steps from initiating the database.

        Raises
        ------
        ValueError
            If the argument 'steps' is not a list or a comma-separated string
        """
        STEPS = ["update_meta", "update_raw", "update_ma_raster",
                 "quality_check", "fillup", "update_richter_class", "richter_correct",
                 "vacuum", "create_ts"]
        # get steps from cli arguments
        steps = cliargs.steps
        if steps == "all":
            steps = STEPS.copy()
        else:
            if type(steps) is str:
                steps = steps.split(",")
            if type(steps) is not list:
                raise ValueError("Argument 'steps' must be a list or a comma-separated string.")

            # order steps
            steps = [step for step in STEPS if step in steps]

            # check if lower steps have to run
            highest_step = self.broker.get_setting("highest_run_step")
            if highest_step is None:
                self.log.debug("Adding all lower steps to run...")
                steps = STEPS[:STEPS.index(steps[-1])+1]
            else:
                if (STEPS.index(highest_step)+1) < STEPS.index(steps[0]):
                    self.log.debug("Adding lower steps to run...")
                    steps = STEPS[STEPS.index(highest_step)+1:STEPS.index(steps[-1])+1]

        # run steps
        for step in steps:
            # run step
            try:
                if not cliargs.just_check:
                    getattr(self, f"step_{step}")(
                        stids=self.test_stids,
                        skip_missing_stids=True
                    )
            except Exception as e:
                self.log.exception(e)
                self.fail("{} failed ({}: {})".format(step, type(e), e))

            # check step
            self.log.debug(f"Starting Checks for step '{step}'...")
            with self.subTest(step=step):
                getattr(self, f"check_{step}")()
                self.check_broker_inactive()
                self.check_no_error_log()

            # save highest run step
            if len(self.test_result.failures) == 0:
                self.log.debug(f"Setting highest run step to {step}")
                self.broker.set_setting("highest_run_step", step)
            else:
                self.log.debug(f"Step {step} failed. Highest run step is still {self.broker.get_setting('highest_run_step')}")
                break

# cli entry point
if __name__ == "__main__":
    unittest.main()