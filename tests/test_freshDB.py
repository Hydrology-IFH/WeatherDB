from pathlib import Path
import unittest
from unittest.mock import patch
import sys
import argparse

import os
import weatherDB as wdb

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# get cli variables
parser = argparse.ArgumentParser(description="FreshDB Test CLI arguments")
parser.add_argument(
    "--complete",
    action="store_true",
    default=os.environ.get("WEATHERDB_TEST_COMPLETE", False))
cliargs, remaining_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + remaining_args
do_complete = cliargs.complete

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
class FreshDBTestCases(BaseTestCases):

    @classmethod
    def setUpClass(cls):
        cls.empty_db()
        cls.broker.create_db_schema(if_exists="DROP")

    @unittest.skipIf(not (do_complete or not ma_rasters_available()),
                     "Using cached multi anual raster files, as 'WEATHERDB_TEST_COMPLETE' is not set or False and no system argument \"--complete\" was given.")
    def test_download_ma_rasters(self):
        self.log.debug("Downloading multi annual raster files...")
        from weatherDB.utils.get_data import download_ma_rasters
        with patch("builtins.input", return_value="y"):
            download_ma_rasters(
                overwrite=True,
                update_user_config=True,
                which=["hyras", "dwd"])

# cli entry point
if __name__ == "__main__":
    unittest.main()