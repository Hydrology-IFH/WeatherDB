import unittest
import sys
import sqlalchemy as sa
from pathlib import Path

import weatherDB as wdb

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# define TestCases class
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