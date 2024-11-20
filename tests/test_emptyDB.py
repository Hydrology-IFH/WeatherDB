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
    def setUp(cls):
        cls.empty_db()

    @classmethod
    def tearDown(cls):
        cls.empty_db()

    @patch('builtins.input', return_value='D')
    def test_create_db_schema(self, input):
        # method call
        self.broker.create_db_schema()

        # tests
        inspect = sa.inspect(self.db_engine.engine)
        for table in models.ModelBase.metadata.tables.values():
            self.assertTrue(
               inspect.has_table(table.name),
               msg=f"Table \"{table}\" not found in database.")
        for view in models.ModelBase.metadata.views:
            self.assertTrue(
               inspect.has_table(view.__tablename__),
               msg=f"View \"{view.__tablename__}\" not found in database.")

    def test_upgrade_db_schema(self):
        # method call
        self.broker.create_db_schema()
        self.broker.upgrade_db_schema(revision='V1.0.0')
        self.broker.upgrade_db_schema(revision='head')

        # tests
        inspect = sa.inspect(self.db_engine.engine)
        for table in models.ModelBase.metadata.tables.values():
            self.assertTrue(
               inspect.has_table(table.name),
               msg=f"Table \"{table}\" not found in database.")
        for view in models.ModelBase.metadata.views:
            self.assertTrue(
               inspect.has_table(view.__tablename__),
               msg=f"View \"{view.__tablename__}\" not found in database.")

# cli entry point
if __name__ == "__main__":
    unittest.main()