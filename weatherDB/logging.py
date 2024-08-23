import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from pathlib import Path
import re
import socket
import os

# set the log
#############
log = logging.getLogger(__name__.split(".")[0])

LOGDIR = Path(__file__).resolve().parent.joinpath("logs")

def remove_old_logs(max_days=14):
    # remove old logs
    log_tstp = datetime.datetime.now().strftime("%Y%m%d")
    log_date_min = datetime.datetime.now() - datetime.timedelta(days=max_days)
    for log_file in [
            file for file in LOGDIR.glob("*.log.*")
                if re.match(".*\.log\.\d{4}-\d{2}-\d{2}$", file.name)]:
        try:
            file_date = datetime.datetime.strptime(log_file.name.split(".")[-1], "%Y-%m-%d")
            if file_date < log_date_min:
                log_file.unlink()
        except:
            pass

def setup_file_logging():
    if not LOGDIR.is_dir(): LOGDIR.mkdir()
    # add filehandler if necessary
    if not log.hasHandlers():
        log.setLevel(logging.DEBUG)
        try:
            user = os.getlogin()
        except:
            user = "anonym"
        #print(os.getlogin())
        fh = TimedRotatingFileHandler(
            LOGDIR.joinpath(
                "weatherDB_" +
                socket.gethostname().replace(".","_") + 
                f"_{user}.log"),
            when="midnight", encoding="utf-8")
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(
            logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'))
        log.addHandler(fh)