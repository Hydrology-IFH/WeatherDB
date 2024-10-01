import unittest
import sys
import sqlalchemy as sa
import argparse
from pathlib import Path
import pandas as pd

from weatherDB.db import models

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# get cli variables
parser = argparse.ArgumentParser(description="InitDB Test CLI arguments")
parser.add_argument("--steps", default="all",
                    help="The steps to run. Default is 'all'.")
parser.add_argument("--just-check", action="store_true",
                    help="Should only the check be run, without the weatehrDB step method.")
cliargs, remaining_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + remaining_args
print(cliargs)

# define TestCases class
class InitDBTestCases(BaseTestCases):

    @classmethod
    def setUpClass(cls):
        if cliargs.steps == "all":
            cls.empty_db()
            cls.broker.create_db_schema(if_exists="DROP")
        else:
            cls.broker.create_db_schema(if_exists="IGNORE")
            cls.log.debug("Working with previous database state.")

    def run(self, result=None):
        if result is None:
            result = self.defaultTestResult()
        self._resultForDoCleanups = result
        self.test_result = result  # Store the result object
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

    def check_update_meta(self):
        with self.db_engine.connect() as conn:
            for model, n_expct in zip([models.MetaP, models.MetaT, models.MetaPD, models.MetaET],
                                      [7, 6, 8, 3]):
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
                            df_meta.loc[stid, "raw_from"],
                            msg="The raw data starts before the raw_from date in the meta data.")
                        self.assertLessEqual(
                            df_no_na.index.max(),
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
                    self.assertGreaterEqual(
                        fperiod_raw,
                        fperiod_qc,
                        msg="Quality checked timeserie is larger than raw data.")
                    if not fperiod_raw.is_empty():
                        self.assertFalse(
                            fperiod_qc.is_empty(),
                            msg="Quality checked timeserie is empty.")
        #TODO: implement quality check tests
        pass

    def check_fillup(self):
        #TODO: implement fillup tests
        pass

    def check_richter_correct(self):
        #TODO: implement richter correct tests
        pass

    def test_steps(self):
        """Test the single steps from initiating the database.

        Raises
        ------
        ValueError
            If the argument 'steps' is not a list or a comma-separated string
        """
        STEPS = ["update_meta", "update_raw", "update_ma_raster",
                 "quality_check", "fillup", "update_richter_class", "richter_correct"]
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

# cli entry point
if __name__ == "__main__":
    unittest.main()