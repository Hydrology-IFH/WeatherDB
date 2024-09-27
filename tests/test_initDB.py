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
cliargs, remaining_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + remaining_args

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
        self.broker.stations_n.update_richter_class(
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
        from weatherDB.station.StationBases import StationCanVirtualBase
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

            # get raw data
            df_raw = stats.get_df(
                kinds="raw",
                stids=self.test_stids,
                skip_missing_stids=True)

            # check number of stations in df_raw
            with self.subTest(msg="Check number of stations in df_raw"):
                if isinstance(stats._StationClass, StationCanVirtualBase):
                    max_stids = len(self.test_stids)
                else:
                    max_stids = len(stats.get_stations(
                        stids=self.test_stids,
                        skip_missing_stids=True)
                    )
                self.assertEqual(
                    len(df_raw.columns),
                    max_stids,
                    msg=f"Number of {stats._para_long} stations in raw data does not match number of test stations.")

            # check for NA values
            with self.subTest(msg="Check if not only NA values"):
                self.assertFalse(
                    df_raw.isna().all().all(),
                    msg=f"The raw data is NA for every {stats._para_long} station.")

            # check for time range
            with self.subTest(msg="Check time range"):
                self.assertGreaterEqual(
                    df_raw.index.min(),
                    pd.Timestamp("1994-01-01 00:00:00+0000"),
                    msg=f"The raw data for {stats._para_long} stations starts before 1994-01-01.")

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
        pass

    def check_fillup(self):
        pass

    def check_richter_correct(self):
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
            try:
                getattr(self, f"step_{step}")(
                    stids=self.test_stids,
                    skip_missing_stids=True
                )
            except Exception as e:
                self.log.exception(e)
                self.fail("{} failed ({}: {})".format(step, type(e), e))

            with self.subTest(msg=f"Check step: {step}"):
                getattr(self, f"check_{step}")()

            with self.subTest(msg=f"Check broker active state after step: {step}"):
                self.check_broker_inactive()

            with self.subTest(msg=f"Check if error logs in step: {step}"):
                self.check_no_error_log()

            # save highest run step
            if len(self.test_result.failures) == 0:
                self.log.debug(f"Setting highest run step to {step}")
                self.broker.set_setting("highest_run_step", step)

# cli entry point
if __name__ == "__main__":
    unittest.main()