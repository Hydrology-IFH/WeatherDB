import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from pathlib import Path
import re
import socket

__author__ = "Max Schmit"
__email__ = "max.schmit@hydrology.uni-freiburg.de"
__copyright__ = "Copyright 2022, Max Schmit"
__version__ = "0.0.5"

# set the log
#############
log = logging.getLogger(__name__)
log_tstp = datetime.datetime.now().strftime("%Y%m%d")
log_dir = Path(__file__).resolve().parent.joinpath("logs")
if not log_dir.is_dir(): log_dir.mkdir()

# remove old logs
log_date_min = datetime.datetime.now() - datetime.timedelta(days=14)
for log_file in [
        file for file in log_dir.glob("*.log.*") 
             if re.match(".*\.log\.\d{4}-\d{2}-\d{2}$", file.name)]:
    try:
        file_date = datetime.datetime.strptime(log_file.stem.split(".")[2], "%Y-%m-%d")
        if file_date < log_date_min:
            log_file.unlink()
    except:
        pass

# add filehandler if necessary
if not log.hasHandlers():
    log.setLevel(logging.DEBUG)
    fh = TimedRotatingFileHandler(
        log_dir.joinpath(
            "classes_" + 
            socket.gethostname().replace(".","_") + 
            ".log"), 
        when="midnight", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(
        logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
    log.addHandler(fh)

from . import station, stations, broker