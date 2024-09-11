import unittest
import sys
import sqlalchemy as sa
import argparse
from pathlib import Path
import os

import weatherDB as wdb
from weatherDB.db import models

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# set test database connection
if wdb.config.has_section("database:test"):
    wdb.config.set("database", "connection", "test")
elif os.environ.get("DOCKER_ENV", None) == "test":
    wdb.config.set("database", "connection", "environment_variables")
else:
    raise ValueError("No test database credentials found in environment variables or configuration file.")

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
            cls.log.debug("Working with previous database state.")

    # steps for initiating the database
    def step_update_meta(self, **kwargs):
        self.broker.update_meta(**kwargs)

    def step_update_raw(self, **kwargs):
        self.broker.update_raw(**kwargs)

    def step_update_ma(self, **kwargs):
        self.broker.update_ma(**kwargs)

    def step_update_richter_class(self, **kwargs):
        self.broker.stations_n.update_richter_class(**kwargs)

    def step_quality_check(self, **kwargs):
        self.broker.quality_check(**kwargs)

    def step_fillup(self, **kwargs):
        self.broker.fillup(**kwargs)

    def step_richter_correct(self, **kwargs):
        self.broker.richter_correct(**kwargs)

    def check_update_meta(self):
        with self.db_engine.connect() as conn:
            for model, n_expct in zip([models.MetaN, models.MetaT, models.MetaND, models.MetaET],
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
                if model != models.MetaND:
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
            with self.subTest(msg="check last_imp_corr for MetaN"):
                stmnt = sa.select(sa.func.count("*"))\
                    .select_from(models.MetaN)\
                    .where(models.MetaN.last_imp_corr)
                self.assertEqual(
                    conn.execute(stmnt).scalar(),
                    0,
                    msg="Some stations have last_imp_corr=True in meta data."
                )

    def check_update_ma(self):
        for stats in self.broker.stations:
            for stat in stats.get_stations(stids=self.test_stids):
                mas = stat.get_ma()
                with self.subTest(stat=stat, mas=mas):
                    self.assertTrue(
                        all([ma is not None for ma in mas]),
                        msg=f"Station {stat.id} of {stat._para_long} Station has no multi annual data.")

    def check_update_raw(self):
        for stats in self.broker.stations:
            for stat in stats:
                raw = stat.get_raw(stids=self.test_stids)
                self.assertEqual(
                    len(raw),
                    len(self.test_stids),
                    msg="Number of stations in raw data does not match number of test stations.")

    def check_update_richter_class(self):
        with self.db_engine.connect() as conn:
            for model in [models.MetaN, models.MetaND]:
                with self.subTest(model=model):
                    self.assertEqual(
                        conn.execute(sa.select(model).where(model.richter_class == 'NULL').count()),
                        0,
                        msg="Some stations don't have a richter_class in meta data."
                    )

    def check_quality_check(self):
        pass

    def check_fillup(self):
        pass

    def check_richter_class(self):
        pass

    def test_steps(self):
        """Test the single steps from initiating the database.

        Raises
        ------
        ValueError
            If the argument 'steps' is not a list or a comma-separated string
        """
        STEPS = ["update_meta", "update_raw", "update_ma", "update_richter_class", "quality_check", "fillup", "richter_correct"]
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
                steps = STEPS[:STEPS.index(steps[-1])]
            else:
                if (STEPS.index(highest_step)+1) < STEPS.index(steps[0]):
                    self.log.debug("Adding lower steps to run...")
                    steps = STEPS[STEPS.index(highest_step)+1:STEPS.index(steps[-1])+1]

        # run steps
        for step in steps:
            try:
                getattr(self, f"step_{step}")(stids=self.test_stids)
            except Exception as e:
                self.log.exception(e)
                self.fail("{} failed ({}: {})".format(step, type(e), e))

            with self.subTest(step=step):
                getattr(self, f"check_{step}")()
                self.check_broker_inactive()

        # save highest run step
        self.broker.set_setting("highest_run_step", steps[-1])

class FilledTestCases(BaseTestCases):
    @classmethod
    def setUpClass(cls):
        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.DropSchema("public", cascade=True, if_exists=True))
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True, if_exists=True))

        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.CreateSchema("public", if_not_exists=True))

        wdb.config.set("database", "connection", "test")

    @classmethod
    def tearDownClass(cls):
        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.DropSchema("public", cascade=True))
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True))

# cli entry point
if __name__ == "__main__":
    unittest.main()