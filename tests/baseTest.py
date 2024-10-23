import unittest
import sqlalchemy as sa
import logging
import os

import weatherDB as wdb
from weatherDB.db import models

# setup logging handler for testing
class ListHandler(logging.Handler):
    def __init__(self):
        super().__init__()
        self.log_list = []

    def emit(self, record):
        self.log_list.append(self.format(record))

# set test database connection
if wdb.config.has_section("database:test"):
    wdb.config.set("database", "connection", "test")
elif os.environ.get("DOCKER_ENV", None) == "test":
    wdb.config.set("database", "connection", "environment_variables")
else:
    raise ValueError("No test database credentials found in environment variables or configuration file.")

# define TestCases classes
class BaseTestCases(unittest.TestCase):
    broker = wdb.broker.Broker()
    db_engine = wdb.db.connections.db_engine
    test_stids = [1224, 1443, 2388, 7243, 1346, 684, 757]
    log = wdb.utils.logging.log

    @staticmethod
    def empty_db():
        with wdb.db.connections.db_engine.connect() as conn:
            models.ModelBase.metadata.drop_all(conn)
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True, if_exists=True))
            conn.execute(sa.text("DROP TABLE IF EXISTS alembic_version CASCADE;"))
            conn.commit()

    def setUp(self) -> None:
        self.log_capturer = ListHandler()
        self.log_capturer.setLevel(logging.ERROR)
        self.log.addHandler(self.log_capturer)
        return super().setUp()

    def tearDown(self) -> None:
        self.log.removeHandler(self.log_capturer)
        return super().tearDown()

    def check_no_error_log(self):
        self.assertEqual(
            len(self.log_capturer.log_list),
            0,
            msg="Errors occurred during test execution.")

    def check_broker_inactive(self):
        self.assertFalse(self.broker.is_any_active, msg="Broker is still marked as active.")

class FreshDBTestCases(BaseTestCases):

    @classmethod
    def setUpClass(cls):
        cls.empty_db()
        cls.broker.create_db_schema(if_exists="DROP")