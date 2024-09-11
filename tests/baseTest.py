from pathlib import Path
import unittest
import sqlalchemy as sa

import os
import weatherDB as wdb
from weatherDB.db import models

# set test database connection
if wdb.config.has_section("database:test"):
    wdb.config.set("database", "connection", "test")
elif os.environ.get("DOCKER_ENV", None) == "test":
    wdb.config.set("database", "connection", "environment_variables")
else:
    raise ValueError("No test database credentials found in environment variables or configuration file.")

# define TestCases class
class BaseTestCases(unittest.TestCase):
    broker = wdb.broker.Broker()
    db_engine = wdb.db.connections.db_engine
    test_stids = [1224, 1443, 2388, 7243, 1346, 5049, 684, 757]
    log = wdb.utils.logging.log

    @staticmethod
    def empty_db():
        with wdb.db.connections.db_engine.connect() as conn:
            models.Base.metadata.drop_all(conn)
            conn.execute(sa.schema.DropSchema("timeseries", cascade=True, if_exists=True))
            conn.commit()

    def check_broker_inactive(self):
        self.assertFalse(self.broker.is_broker_active, msg="Broker is still marked as active.")
