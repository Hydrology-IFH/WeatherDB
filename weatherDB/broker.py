"""
This submodule has only one class Broker. This one is used to do actions on all the stations together. Mainly only used for updating the DB.
"""
# libraries
import logging
from sqlalchemy import text as sqltxt
from packaging import version as pv
from pathlib import Path
import textwrap
from contextlib import contextmanager

from .db.connections import db_engine
from .config import config
from .stations import StationsP, StationsPD, StationsT, StationsET
from . import __version__ as __version__

__all__ = ["Broker"]

log = logging.getLogger(__name__)

class Broker(object):
    """A class to manage and update the database.

    Can get used to update all the stations and parameters at once.

    This class is only working with SELECT and UPDATE user privileges. Even Better is to also have DELETE and INSERT privileges.
    """
    def __init__(self):
        if not db_engine.update_privilege & db_engine.select_privilege:
            raise PermissionError("You don't have enough privileges to use the Broker. The database user must have SELECT and UPDATE privileges.")

        self.stations_pd = StationsPD()
        self.stations_t = StationsT()
        self.stations_et = StationsET()
        self.stations_p = StationsP()
        self.stations = [
            self.stations_pd,
            self.stations_t,
            self.stations_et,
            self.stations_p]

    def _check_paras(self, paras, valid_paras=["p_d", "p", "t", "et"]):
        valid_paras = ["p_d", "p", "t", "et"]
        for para in paras:
            if para not in valid_paras:
                raise ValueError(
                    "The given parameter {para} is not valid.".format(
                        para=para))

    def update_raw(self, only_new=True, paras=["p_d", "p", "t", "et"], **kwargs):
        """Update the raw data from the DWD-CDC server to the database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True.
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to the update_raw method of the stations
        """
        self._check_paras(paras)
        log.info("="*79 + "\nBroker update_raw starts")

        with self.activate():
            for stations in self.stations:
                if stations._para in paras:
                    stations.update_raw(only_new=only_new, **kwargs)

    def update_meta(self, paras=["p_d", "p", "t", "et"], **kwargs):
        """Update the meta file from the CDC Server to the Database.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_meta method of the stations
        """
        self._check_paras(paras)
        log.info("="*79 + "\nBroker update_meta starts")

        with self.activate():
            for stations in self.stations:
                if stations._para in paras:
                    stations.update_meta(**kwargs)

    def update_ma_raster(self, paras=["p_d", "p", "t", "et"], **kwargs):
        """Update the multi-annual data from raster to table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_ma_raster method of the stations
        """
        self._check_paras(paras)
        log.info("="*79 + "\nBroker update_ma_raster starts")

        with self.activate():
            for stations in self.stations:
                if stations._para in paras:
                    stations.update_ma_raster(**kwargs)

    def update_period_meta(self, paras=["p_d", "p", "t", "et"], **kwargs):
        """Update the periods in the meta table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_period_meta method of the stations
        """
        self._check_paras(paras=paras,
                          valid_paras=["p_d", "p", "t", "et"])
        log.info("="*79 + "\nBroker update_period_meta starts")
        with self.activate():
            for stations in self.stations:
                if stations._para in paras:
                    stations.update_period_meta(**kwargs)

    def quality_check(self, paras=["p", "t", "et"], with_fillup_nd=True, **kwargs):
        """Do the quality check on the stations raw data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p", "t", "et"].
            The default is ["p", "t", "et"].
        with_fillup_nd : bool, optional
            Should the daily precipitation data get filled up if the 10 minute precipitation data gets quality checked.
            The default is True.
        **kwargs : dict
            The keyword arguments to pass to quality_check method of the stations
        """
        self._check_paras(
            paras=paras,
            valid_paras=["p", "t", "et"])
        log.info("="*79 + "\nBroker quality_check starts")

        with self.activate():
            if with_fillup_nd and "p" in paras:
                self.stations_pd.fillup(**kwargs)

            for stations in self.stations:
                if stations._para in paras:
                    stations.quality_check(**kwargs)

    def last_imp_quality_check(self, paras=["p", "t", "et"], with_fillup_nd=True, **kwargs):
        """Quality check the last imported data.

        Also fills up the daily precipitation data if the 10 minute precipitation data should get quality checked.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p", "t", "et"].
            The default is ["p", "t", "et"].
        with_fillup_nd : bool, optional
            Should the daily precipitation data get filled up if the 10 minute precipitation data gets quality checked.
            The default is True.
        **kwargs : dict
            The keyword arguments to pass to last_imp_quality_check method of the stations.
            If with_fillup_nd is True, the keyword arguments are also passed to the last_imp_fillup method of the stations_pd.
        """
        self._check_paras(
                paras=paras,
                valid_paras=["p", "t", "et"])
        log.info("="*79 + "\nBroker last_imp_quality_check starts")

        with self.activate():
            if with_fillup_nd and "p" in paras:
                self.stations_pd.last_imp_fillup(**kwargs)

            for stations in self.stations:
                if stations._para in paras:
                    stations.last_imp_quality_check(**kwargs)

    def fillup(self, paras=["p", "t", "et"], **kwargs):
        """Fillup the timeseries.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to fillup method of the stations
        """
        log.info("="*79 + "\nBroker fillup starts")
        with self.activate():
            self._check_paras(paras)
            for stations in self.stations:
                if stations._para in paras:
                    stations.fillup(**kwargs)

    def last_imp_fillup(self, paras=["p", "t", "et"], **kwargs):
        """Fillup the last imported data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to last_imp_fillup method of the stations
        """
        log.info("="*79 + "\nBroker last_imp_fillup starts")
        with self.activate():
            self._check_paras(paras)
            for stations in self.stations:
                if stations._para in paras:
                    stations.last_imp_fillup(**kwargs)

    def richter_correct(self, **kwargs):
        """Richter correct all of the precipitation data.

        Parameters
        ----------
        **kwargs : dict
            The keyword arguments to pass to richter_correct method of the stations_p
        """
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        with self.activate():
            self.stations_p.richter_correct(**kwargs)

    def last_imp_corr(self, **kwargs):
        """Richter correct the last imported precipitation data.

        Parameters
        ----------
        **kwargs : dict
            The keyword arguments to pass to last_imp_corr method of the stations
        """
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        with self.activate():
            self.stations_p.last_imp_corr(**kwargs)

    def update_db(self, paras=["p_d", "p", "t", "et"], **kwargs):
        """The regular Update of the database.

        Downloads new data.
        Quality checks the newly imported data.
        Fills up the newly imported data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["p_d", "p", "t", "et"].
            The default is ["p_d", "p", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to the called methods of the stations
        """
        log.info("="*79 + "\nBroker update_db starts")
        self._check_paras(paras)
        with self.activate():
            if pv.parse(__version__) > self.get_db_version():
                log.info("--> There is a new version of the python script. Therefor the database is recalculated completly")
                self.initiate_db()
            else:
                self.update_meta(paras=paras, **kwargs)
                self.update_raw(paras=paras, **kwargs)
                if "p_d" in paras:
                    paras.remove("p_d")
                self.last_imp_quality_check(paras=paras, **kwargs)
                self.last_imp_fillup(paras=paras, **kwargs)
                self.last_imp_corr(**kwargs)

    @db_engine.deco_is_superuser
    def create_db_schema(self, if_exists=None, silent=False):
        """Create the database schema.

        Parameters
        ----------
        if_exists : str, optional
            What to do if the tables already exist.
            If None the user gets asked.
            If "D" or "drop" the tables get dropped and recreated.
            If "I" or "ignore" the existing tables get ignored and the creation of the schema continues for the other.
            If "E" er "exit" the creation of the schema gets exited.
            The default is None.
        silent : bool, optional
            If True the user gets not asked if the tables already exist.
            If True, if_exists must not be None.
            The default is False.
        """
        # check silent
        if silent and if_exists is None:
            raise ValueError("silent can only be True if if_exists is not None.")

        # add POSTGIS extension
        with db_engine.connect() as con:
            con.execute(sqltxt("CREATE EXTENSION IF NOT EXISTS postgis;"))
            con.commit()

        # check for existing tables
        from .db.models import ModelBase
        with db_engine.connect() as con:
            res = con.execute(sqltxt("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE';""")
                ).fetchall()
            existing_tables = [table[0] for table in res]
            problem_tables = [table.name
                              for table in ModelBase.metadata.tables.values()
                              if table.name in existing_tables]
        if len(problem_tables)>0 and silent:
            log.info("The following tables already exist on the database:\n - " +
                  "\n - ".join([table for table in problem_tables]))

            # ask the user what to do if if_exists is None
            if if_exists is None:
                print(textwrap.dedent(
                    """What do you want to do?
                    - [D] : Drop all the tables and recreate them again.
                    - [I] : Ignore those tables and continue with the creation of the schema.
                    - [E] : Exit the creation of the schema."""))
                while True:
                    answer = input("Your choice: ").upper()
                    if answer in ["D", "E", "I"]:
                        if_exists = answer
                        break
                    else:
                        print("Please enter a valid answer.")

            # execute the choice
            if if_exists.upper() == "D":
                log.info("Dropping the tables.")
                with db_engine.connect() as con:
                    for table in problem_tables:
                        con.execute(sqltxt(f"DROP TABLE {table} CASCADE;"))
                    con.commit()
            elif if_exists.upper() == "E":
                log.info("Exiting the creation of the schema.")
                return

        # create the tables
        log.info("Creating the tables and views.")
        with db_engine.connect() as con:
            ModelBase.metadata.create_all(con, checkfirst=True)
            con.commit()

        # create the schema for the timeseries
        with db_engine.connect() as con:
            con.execute(sqltxt("CREATE SCHEMA IF NOT EXISTS timeseries;"))
            con.commit()

        # tell alembic that the actual database schema is up-to-date
        from alembic.config import Config
        from alembic import command
        alembic_cfg = Config(
            Path(config.get("main", "module_path"))/"alembic"/"alembic.ini",
            attributes={"engine": db_engine.get_engine()})
        command.stamp(alembic_cfg, "head")

    @db_engine.deco_all_privileges
    def initiate_db(self, **kwargs):
        """Initiate the Database.

        Downloads all the data from the CDC server for the first time.
        Updates the multi-annual data and the richter-class for all the stations.
        Quality checks and fills up the timeseries.

        Parameters
        ----------
        **kwargs : dict
            The keyword arguments to pass to the called methods of the stations
        """
        log.info("="*79 + "\nBroker initiate_db starts")
        with self.activate():
            #TODO: add a check for alembic version and if the database is up-to-date
            self.update_meta(
                paras=["p_d", "p", "t", "et"], **kwargs)
            self.update_raw(
                paras=["p_d", "p", "t", "et"],
                only_new=False,
                **kwargs)
            self.update_ma_raster(
                paras=["p_d", "p", "t", "et"],
                **kwargs)
            self.stations_p.update_richter_class(**kwargs)
            self.quality_check(paras=["p", "t", "et"], **kwargs)
            self.fillup(paras=["p", "t", "et"], **kwargs)
            self.richter_correct(**kwargs)

            self.set_db_version()

    def vacuum(self, do_analyze=True):
        sql = "VACUUM {analyze};".format(
            analyze="ANALYZE" if do_analyze else "")
        with db_engine.connect() as con:
            con.execute(sqltxt(sql))

    def get_setting(self, key):
        """Get a specific settings value from the database.

        Parameters
        ----------
        key : str
            The key of the setting.

        Returns
        -------
        value: str or None
            The database settings value.
        """
        with db_engine.connect() as con:
            res = con.execute(
                sqltxt(f"SELECT value FROM settings WHERE key='{key}';")
                ).fetchone()
        if res is None:
            return None
        else:
            return res[0]

    def set_setting(self, key:str, value:str):
        """Set a specific setting.

        Parameters
        ----------
        key : str
            The key of the setting.
        value : str
            The value of the setting.
        """
        with db_engine.connect() as con:
            con.execute(sqltxt(
                f"""INSERT INTO settings
                VALUES ('{key}', '{value}')
                ON CONFLICT (key)
                    DO UPDATE SET value=EXCLUDED.value;"""))
            con.commit()

    def get_db_version(self):
        """Get the package version that the databases state is at.

        Returns
        -------
        version
            The version of the database.
        """
        res = self.get_setting("version")
        if res is not None:
            res = pv.parse(res)
        return res

    def set_db_version(self, version=pv.parse(__version__)):
        """Set the package version that the databases state is at.

        Parameters
        ----------
        version: pv.Version, optional
            The Version of the python package
            The default is the version of this package.
        """
        if not isinstance(version, pv.Version):
            raise TypeError("version must be of type pv.Version")
        self.set_setting("version", str(version))

    @property
    def is_broker_active(self):
        """Get the state of the broker.

        Returns
        -------
        bool
            Whether the broker is active.
        """
        return self.get_setting("is_broker_active") == "True"

    @is_broker_active.setter
    def is_broker_active(self, is_active:bool):
        """Set the state of the broker.

        Parameters
        ----------
        is_active : bool
            Whether the broker is active.
        """
        self.set_setting("is_broker_active", str(is_active))

    @contextmanager
    def activate(self):
        """Activate the broker in a context manager."""
        try:
            if self.is_broker_active:
                raise RuntimeError("Another Broker is active and therefor this broker is not allowed to run.")
            self.is_broker_active = True
            yield self
        finally:
            self.is_broker_active = False