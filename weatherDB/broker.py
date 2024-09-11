"""
This submodule has only one class Broker. This one is used to do actions on all the stations together. Mainly only used for updating the DB.
"""
# libraries
import logging
from sqlalchemy import text as sqltxt
from packaging import version as pv
from pathlib import Path
import textwrap

from .db.connections import db_engine
from .config import config
from .stations import StationsN, StationsND, StationsT, StationsET
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

        self.stations_nd = StationsND()
        self.stations_t = StationsT()
        self.stations_et = StationsET()
        self.stations_n = StationsN()
        self.stations = [
            self.stations_nd,
            self.stations_t,
            self.stations_et,
            self.stations_n]

    def _check_paras(self, paras, valid_paras=["n_d", "n", "t", "et"]):
        valid_paras = ["n_d", "n", "t", "et"]
        for para in paras:
            if para not in valid_paras:
                raise ValueError(
                    "The given parameter {para} is not valid.".format(
                        para=para))

    def update_raw(self, only_new=True, paras=["n_d", "n", "t", "et"], **kwargs):
        """Update the raw data from the DWD-CDC server to the database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True.
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to the update_raw method of the stations
        """
        self.check_is_broker_active()
        log.info("="*79 + "\nBroker update_raw starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_raw(only_new=only_new, **kwargs)
        self.set_is_broker_active(False)

    def update_meta(self, paras=["n_d", "n", "t", "et"], **kwargs):
        """Update the meta file from the CDC Server to the Database.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_meta method of the stations
        """
        log.info("="*79 + "\nBroker update_meta starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_meta(**kwargs)

    def update_ma(self, paras=["n_d", "n", "t", "et"], **kwargs):
        """Update the multi-annual data from raster to table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_ma method of the stations
        """
        log.info("="*79 + "\nBroker update_ma starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_ma(**kwargs)

    def update_period_meta(self, paras=["n_d", "n", "t", "et"], **kwargs):
        """Update the periods in the meta table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to update_period_meta method of the stations
        """
        self._check_paras(paras=paras,
                          valid_paras=["n_d", "n", "t", "et"])
        log.info("="*79 + "\nBroker update_period_meta starts")

        for stations in self.stations:
            if stations._para in paras:
                stations.update_period_meta(**kwargs)

    def quality_check(self, paras=["n", "t", "et"], with_fillup_nd=True, **kwargs):
        """Do the quality check on the stations raw data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n", "t", "et"].
            The default is ["n", "t", "et"].
        with_fillup_nd : bool, optional
            Should the daily precipitation data get filled up if the 10 minute precipitation data gets quality checked.
            The default is True.
        **kwargs : dict
            The keyword arguments to pass to quality_check method of the stations
        """
        self.check_is_broker_active()
        self._check_paras(paras=paras, valid_paras=["n", "t", "et"])
        log.info("="*79 + "\nBroker quality_check starts")

        if with_fillup_nd and "n" in paras:
            self.stations_nd.fillup(**kwargs)

        for stations in self.stations:
            if stations._para in paras:
                stations.quality_check(**kwargs)
        self.set_is_broker_active(False)

    def last_imp_quality_check(self, paras=["n", "t", "et"], with_fillup_nd=True, **kwargs):
        """Quality check the last imported data.

        Also fills up the daily precipitation data if the 10 minute precipitation data should get quality checked.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n", "t", "et"].
            The default is ["n", "t", "et"].
        with_fillup_nd : bool, optional
            Should the daily precipitation data get filled up if the 10 minute precipitation data gets quality checked.
            The default is True.
        **kwargs : dict
            The keyword arguments to pass to last_imp_quality_check method of the stations.
            If with_fillup_nd is True, the keyword arguments are also passed to the last_imp_fillup method of the stations_nd.
        """
        log.info("="*79 + "\nBroker last_imp_quality_check starts")
        self._check_paras(
            paras=paras,
            valid_paras=["n", "t", "et"])

        if with_fillup_nd and "n" in paras:
            self.stations_nd.last_imp_fillup(**kwargs)

        for stations in self.stations:
            if stations._para in paras:
                stations.last_imp_quality_check(**kwargs)

    def fillup(self, paras=["n", "t", "et"], **kwargs):
        """Fillup the timeseries.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to fillup method of the stations
        """
        self.check_is_broker_active()
        log.info("="*79 + "\nBroker fillup starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.fillup(**kwargs)
        self.set_is_broker_active(False)

    def last_imp_fillup(self, paras=["n", "t", "et"], **kwargs):
        """Fillup the last imported data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to last_imp_fillup method of the stations
        """
        log.info("="*79 + "\nBroker last_imp_fillup starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.last_imp_fillup(**kwargs)

    def richter_correct(self, **kwargs):
        """Richter correct all of the precipitation data.

        Parameters
        ----------
        **kwargs : dict
            The keyword arguments to pass to richter_correct method of the stations_n
        """
        self.check_is_broker_active()
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        self.stations_n.richter_correct(**kwargs)
        self.set_is_broker_active(False)

    def last_imp_corr(self, **kwargs):
        """Richter correct the last imported precipitation data.

        Parameters
        ----------
        **kwargs : dict
            The keyword arguments to pass to last_imp_corr method of the stations
        """
        self.check_is_broker_active()
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        self.stations_n.last_imp_corr(**kwargs)
        self.check_is_broker_active()

    def update_db(self, paras=["n_d", "n", "t", "et"], **kwargs):
        """The regular Update of the database.

        Downloads new data.
        Quality checks the newly imported data.
        Fills up the newly imported data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        **kwargs : dict
            The keyword arguments to pass to the called methods of the stations
        """
        log.info("="*79 + "\nBroker update_db starts")
        self._check_paras(paras)
        self.check_is_broker_active()

        if pv.parse(__version__) > self.get_db_version():
            log.info("--> There is a new version of the python script. Therefor the database is recalculated completly")
            self.initiate_db()
        else:
            self.update_meta(paras=paras, **kwargs)
            self.update_raw(paras=paras, **kwargs)
            if "n_d" in paras:
                paras.remove("n_d")
            self.last_imp_quality_check(paras=paras, **kwargs)
            self.last_imp_fillup(paras=paras, **kwargs)
            self.last_imp_corr(**kwargs)
            self.set_is_broker_active(False)

    @db_engine.deco_is_superuser
    def create_db_schema(self, if_exists=None):
        """Create the database schema.

        Parameters
        ----------
        if_exists : str, optional
            What to do if the tables already exist.
            If None the user gets asked.
            If "D" or "drop" the tables get dropped and recreated.
            If "E" er "exit" the creation of the schema gets exited.
            The default
        """
        # add POSTGIS extension
        with db_engine.connect() as con:
            con.execute(sqltxt("CREATE EXTENSION IF NOT EXISTS postgis;"))
            con.commit()

        # check for existing tables
        from .db.models import Base
        with db_engine.connect() as con:
            res = con.execute(sqltxt(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
                ).fetchall()
            existing_tables = [table[0] for table in res]
            problem_tables = [table for table in Base.metadata.tables
                                    if table in existing_tables]

        if len(problem_tables)>0:
            print("The following tables already exist on the database:\n - " +
                  "\n - ".join([table for table in problem_tables]))

            # ask the user what to do if if_exists is None
            if if_exists is None:
                print(textwrap.dedent(
                    """What do you want to do?
                    - [D] : Drop all the tables and recreate them again.
                    - [E] : Exit the creation of the schema."""))
                while True:
                    answer = input("Your choice: ").upper()
                    if answer == "D" or answer == "E":
                        if_exists = answer
                        break
                    else:
                        print("Please enter a valid answer.")

            # execute the choice
            if if_exists == "D":
                print("Dropping the tables.")
                with db_engine.connect() as con:
                    for table in problem_tables:
                        con.execute(sqltxt(f"DROP TABLE {table} CASCADE;"))
                    con.commit()
            elif if_exists == "E":
                print("Exiting the creation of the schema.")
                return

        # create the tables
        print("Creating the tables.")
        Base.metadata.create_all(db_engine.get_engine())

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
        self.check_is_broker_active()

        self.update_meta(
            paras=["n_d", "n", "t", "et"], **kwargs)
        self.update_raw(
            paras=["n_d", "n", "t", "et"],
            only_new=False,
            **kwargs)
        self.update_ma(
            paras=["n_d", "n", "t", "et"],
            **kwargs)
        self.stations_n.update_richter_class(**kwargs)
        self.quality_check(paras=["n", "t", "et"], **kwargs)
        self.fillup(paras=["n", "t", "et"], **kwargs)
        self.richter_correct(**kwargs)

        self.set_db_version()

        self.set_is_broker_active(False)

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

    def set_is_broker_active(self, is_active:bool):
        """Set the state of the broker.

        Parameters
        ----------
        is_active : bool
            Whether the broker is active.
        """
        self.set_setting("is_broker_active", str(is_active))

    def get_is_broker_active(self):
        """Get the state of the broker.

        Returns
        -------
        bool
            Whether the broker is active.
        """
        return self.get_setting("is_broker_active") == "True"

    def check_is_broker_active(self):
        """Check if another broker instance is active and if so raise an error.

        Raises
        ------
        RuntimeError
            If the broker is not active.
        """
        if self.get_is_broker_active():
            raise RuntimeError("Another Broker is active and therefor this broker is not allowed to run.")
        else:
            self.set_is_broker_active(True)