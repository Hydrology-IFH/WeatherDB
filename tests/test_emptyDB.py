import unittest
from unittest.mock import patch
import sqlalchemy as sa
import sys
from pathlib import Path

from weatherDB.db import models

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# define TestCases class
class EmptyDBTestCases(BaseTestCases):
    @classmethod
    def setUpClass(cls):
        cls.empty_db()

    @classmethod
    def tearDownClass(cls):
        cls.empty_db()

    @patch('builtins.input', return_value='D')
    def test_create_db_schema(self, input):
        # method call
        self.broker.create_db_schema()

        # tests
        inspect = sa.inspect(self.db_engine.engine)
        for table in models.Base.metadata.tables.keys():
           self.assertTrue(
               inspect.has_table(table),
                msg=f"Table \"{table}\" not found in database.")

# cli entry point
if __name__ == "__main__":
    unittest.main()