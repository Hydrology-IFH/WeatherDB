from pathlib import Path
import unittest
from unittest.mock import patch
import sys
import argparse
from shutil import copyfile
from distutils.util import strtobool

import os
import weatherdb as wdb

sys.path.insert(0, Path(__file__).parent.resolve().as_posix())
from baseTest import BaseTestCases

# get cli variables
parser = argparse.ArgumentParser(description="FreshDB Test CLI arguments")
parser.add_argument(
    "--complete",
    action="store_true",
    default=strtobool(os.environ.get("WEATHERDB_TEST_DR_COMPLETE", "False")))
parser.add_argument(
    "--copy-rasters",
    action="store_true",
    default=strtobool(os.environ.get("WEATHERDB_TEST_DR_COPY_RASTERS", "False")),
    help="Copy the test raster files to the base data directory.")
parser.add_argument(
    "--update-user-config",
    action="store_true",
    default=strtobool(os.environ.get("WEATHERDB_TEST_DR_UPDATE_USER_CONFIG", "False")),
    help="Update the user configuration file with the test data configurations.")
cliargs, remaining_args = parser.parse_known_args()
sys.argv = [sys.argv[0]] + remaining_args
do_complete = cliargs.complete

# copy the test datasets before running the tests
if cliargs.copy_rasters:
    print("Copying test data...")
    import configparser
    data_dir = Path(wdb.config.get("data", "base_dir"))
    orig_dir = Path(__file__).parent/"test-data"
    for fp in orig_dir.glob("**/*.tif"):
        dst_fp = data_dir / fp.relative_to(orig_dir)
        if dst_fp.exists():
            dst_fp.unlink()
        dst_fp.parent.mkdir(parents=True, exist_ok=True)
        copyfile(fp, dst_fp)
    if cliargs.update_user_config:
        new_config = configparser.ConfigParser()
        new_config.read(
            orig_dir.joinpath("test-data-config.ini"))
        for section in new_config._sections:
            for key, val in new_config.items(section):
                wdb.config.update_user_config(section, key, val)
    else:
        wdb.config.read(orig_dir.joinpath("test-data-config.ini"))

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

def dem_rasters_available():
    if wdb.config.has_option("data:rasters", "dems"):
        return all([Path(file).exists()
                    for file in wdb.config.get_list("data:rasters", "dems")])
    else:
        return False

# define TestCases class
class DownloadRastersTestCases(BaseTestCases):

    @classmethod
    def setUpClass(cls):
        cls.broker.create_db_schema(if_exists="IGNORE")

    @unittest.skipIf(not (do_complete or not ma_rasters_available()),
                     "Using cached multi anual raster files, as 'WEATHERDB_TEST_COMPLETE' is not set or False and no system argument \"--complete\" was given.")
    def test_download_ma_rasters(self):
        self.log.debug("Downloading multi annual raster files...")
        from weatherdb.utils.get_data import download_ma_rasters
        with patch("builtins.input", return_value="y"):
            download_ma_rasters(
                overwrite=True,
                update_user_config=True,
                which=["hyras", "dwd"])

        # check if the files exists
        self.assertTrue(
            ma_rasters_available(),
            "Some multi annual raster files are missing.")

    @unittest.skipIf(not (do_complete or not dem_rasters_available()),
                    "Using cached DEM raster file, as 'WEATHERDB_TEST_COMPLETE' is not set or False and no system argument \"--complete\" was given.")
    def test_download_dem(self):
        self.log.debug("Downloading DEM raster file...")
        from weatherdb.utils.get_data import download_dem
        with patch("builtins.input", return_value="y"):
            download_dem(
                overwrite=True,
                extent=[7, 47.5, 8.7, 48.5],
                update_user_config=True)

        # check if the file exists
        self.assertTrue(
            dem_rasters_available(),
            "DEM raster file is missing.")

# cli entry point
if __name__ == "__main__":
    unittest.main()