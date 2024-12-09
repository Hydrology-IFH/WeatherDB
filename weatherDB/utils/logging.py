import logging
from logging.handlers import TimedRotatingFileHandler
import datetime
from pathlib import Path
import re
import socket
import os
import gzip
import shutil

from ..config import config

try:
    import coloredlogs
    cl_available = True
except ImportError:
    cl_available = False

# set the log
#############
log = logging.getLogger(__name__.split(".")[0])

def _get_log_dir ():
    return Path(config.get("logging", "directory"))

def remove_old_logs(max_days=14):
    # remove old logs
    log_dir = _get_log_dir()
    log_date_min = datetime.datetime.now() - datetime.timedelta(days=max_days)
    for log_file in [
            file for file in log_dir.glob("*.log.*")
                if re.match(r".*\.log\.\d{4}-\d{2}-\d{2}$", file.name)]:
        try:
            file_date = datetime.datetime.strptime(log_file.name.split(".")[-1], "%Y-%m-%d")
            if file_date < log_date_min:
                log_file.unlink()
        except:
            pass

def setup_logging_handlers():
    """Setup the logging handlers depending on the configuration.

    Raises
    ------
    ValueError
        If the handler type is not known.
    """
    # get log dir
    log_dir = _get_log_dir()
    if not log_dir.is_dir(): log_dir.mkdir()

    # add filehandler if necessary
    log.setLevel(config.get("logging", "level", fallback=logging.DEBUG))
    handler_names = [h.get_name() for h in log.handlers]
    format = config.get(
        "logging",
        "format",
        raw=True,
        fallback="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    level = config.get("logging", "level", fallback=logging.DEBUG)
    for handler_type in config.get_list("logging", "handlers"):
        handler_name = f"weatherDB_config:{handler_type}"

        # check if coloredlogs is available
        if cl_available and handler_type == "console":
            coloredlogs.install(level=level, fmt=format, logger=log)
            log.debug("Using coloredlogs")
            continue

        # get log file name
        if handler_type == "file":
            try:
                user = os.getlogin()
            except:
                user = "anonym"
            host = socket.gethostname().replace(".","_")
            log_file = log_dir.joinpath(
                config.get("logging", "file", fallback="weatherDB_{user}_{host}.log")\
                    .format(user=user, host=host))

        # get or create handler
        if handler_name not in handler_names:
            if handler_type == "console":
                handler = logging.StreamHandler()
            elif handler_type == "file":
                handler = TimedRotatingFileHandler(
                    log_file,
                    when="midnight",
                    encoding="utf-8")
                if config.getboolean("logging", "compression", fallback=True):
                    def namer(name):
                        return name + ".gz"
                    def rotator(source, dest):
                        with open(source, 'rb') as f_in:
                            with gzip.open(dest, 'wb') as f_out:
                                shutil.copyfileobj(f_in, f_out)
                        os.remove(source)
                    handler.namer = namer
                    handler.rotator = rotator
            else:
                raise ValueError(f"Handler '{handler_type}' not known.")

            handler.set_name(handler_name)
            log.addHandler(handler)

        elif handler_name in handler_names:
            handler = log.handlers[handler_names.index(handler_name)]

            # check if file path has changed
            if handler_type == "file" and handler.baseFilename != str(log_file):
                log.removeHandler(handler)
                handler.close()
                handler = TimedRotatingFileHandler(
                    log_file,
                    when="midnight",
                    encoding="utf-8")
                handler.set_name(handler_name)
                log.addHandler(handler)

        # set formatter and level
        handler.setFormatter(
            logging.Formatter(format))
        handler.setLevel(level)

# add config listener
config.add_listener("logging", None, setup_logging_handlers)