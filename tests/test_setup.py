from pathlib import Path
import unittest
from unittest.mock import patch
import sys
import sqlalchemy as sa

import os
import weatherDB as wdb

# set test database connection
if wdb.config.has_section("database:test"):
    wdb.config.set("database", "connection", "test")
elif os.environ.get("DOCKER_ENV", None) == "test":
    wdb.config.set("database", "connection", "environment_variables")
else:
    raise ValueError("No test database credentials found in environment variables or configuration file.")

# get environment variables
if "--complete" in sys.argv:
    do_complete = True
else:
    do_complete = os.environ.get("WEATHERDB_TEST_COMPLETE", False)

# define functions
def ma_rasters_available():
    for key in ["hyras", "dwd"]:
        if wdb.config.has_option(f"data:rasters:{key}", "file"):
            file = Path(wdb.config.get(f"data:rasters:{key}", "file"))
            if not file.exists():
                return False
        else:
            return False
    return True

# define TestCases class
class BaseTestCases(unittest.TestCase):
    broker = wdb.broker.Broker()
    db_engine = wdb.db.connections.db_engine
    test_stids = [1224, 1443, 2388, 7243, 1346, 5049, 684, 757]


class EmptyDBTestCases(BaseTestCases):
    @classmethod
    def setUpClass(cls):
        print("removing schemas")
        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.DropSchema("public", cascade=True, if_exists=True))
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True, if_exists=True))
            conn.commit()

        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.CreateSchema("public", if_not_exists=True))

    @classmethod
    def tearDownClass(cls):
        with cls.db_engine.connect() as conn:
            conn.execute(sa.schema.DropSchema("public", cascade=True, if_exists=True))
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True, if_exists=True))

    @patch('builtins.input', return_value='D')
    def test_create_db_schema(self, input):
        # method call
        self.broker.create_db_schema()

        # tests
        inspect = sa.inspect(self.db_engine.engine)
        for table in wdb.db.models.Base.metadata.tables.keys():
           self.assertTrue(
               inspect.has_table(table),
                msg=f"Table \"{table}\" not found in database.")

    def test_initiate_db(self):
        self.broker.initiate_db(stids=self.test_stids)

class FreshDBTestCases(EmptyDBTestCases):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.broker.create_db_schema()

    def test_download_raw(self):
        # method call
        self.broker.update_raw(stids=self.test_stids)

        # tests
        inspect = sa.inspect(self.db_engine.engine)
        for stid in self.test_stids:
            for para in ["N", "T", "ND", "ET"]:
                self.assertTrue(
                    inspect.has_table(f"{stid}_{para}", schema="timeseries"),
                    msg=f"Timeseries table \"{stid}_{para}\" not found in database.")

    @unittest.skipIf(not (do_complete or not ma_rasters_available()),
                     "Using cached multi anual raster files, as 'WEATHERDB_TEST_COMPLETE' is not set or False and no system argument \"--complete\" was given.")
    def test_download_ma_rasters(self):
        print("Downloading multi annual raster files...")
        print(ma_rasters_available())
        print(do_complete)
        print(not (do_complete or not ma_rasters_available()))
        from weatherDB.utils.get_data import download_ma_rasters
        download_ma_rasters(overwrite=True, update_user_config=True, which=["hyras", "dwd"])

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

if __name__ == "__main__":
    unittest.main()