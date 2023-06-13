"""
This submodule has only one class Broker. This one is used to do actions on all the stations together. Mainly only used for updating the DB.
"""
# libraries
import logging
from sqlalchemy import text as sqltxt
from .lib.connections import DB_ENG
from .stations import StationsN, StationsND, StationsT, StationsET
from packaging import version as pv
from . import __version__ as __version__

log = logging.getLogger(__name__)

if not DB_ENG.is_superuser:
    raise PermissionError("You are no super user of the Database and therefor the Broker class is not available.")

class Broker(object):
    """A class to manage and update the database.

    Can get used to update all the stations and parameters at once.

    This class is only working with super user privileges.
    """
    def __init__(self):
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

    def update_raw(self, only_new=True, paras=["n_d", "n", "t", "et"]):
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
        """
        log.info("="*79 + "\nBroker update_raw starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_raw(only_new=only_new)

    def update_meta(self, paras=["n_d", "n", "t", "et"]):
        """Update the meta file from the CDC Server to the Database.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        """
        log.info("="*79 + "\nBroker update_meta starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_meta()

    def update_ma(self, paras=["n_d", "n", "t", "et"]):
        """Update the multi-annual data from raster to table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        """
        log.info("="*79 + "\nBroker update_ma starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.update_ma()

    def update_period_meta(self, paras=["n_d", "n", "t", "et"]):
        """Update the periods in the meta table.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        """
        self._check_paras(paras=paras,
                          valid_paras=["n_d", "n", "t", "et"])
        log.info("="*79 + "\nBroker update_period_meta starts")

        for stations in self.stations:
            if stations._para in paras:
                stations.update_period_meta()

    def quality_check(self, paras=["n", "t", "et"], with_fillup_nd=True):
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
        """
        self._check_paras(paras=paras, valid_paras=["n", "t", "et"])
        log.info("="*79 + "\nBroker quality_check starts")

        if with_fillup_nd and "n" in paras:
            self.stations_nd.fillup()

        for stations in self.stations:
            if stations._para in paras:
                stations.quality_check()

    def last_imp_quality_check(self, paras=["n", "t", "et"], with_fillup_nd=True):
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
        """
        log.info("="*79 + "\nBroker last_imp_quality_check starts")
        self._check_paras(
            paras=paras,
            valid_paras=["n", "t", "et"])

        if with_fillup_nd and "n" in paras:
            self.stations_nd.last_imp_fillup()

        for stations in self.stations:
            if stations._para in paras:
                stations.last_imp_quality_check()

    def fillup(self, paras=["n", "t", "et"]):
        """Fillup the timeseries.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        """
        log.info("="*79 + "\nBroker fillup starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.fillup()

    def last_imp_fillup(self, paras=["n", "t", "et"]):
        """Fillup the last imported data.

        Parameters
        ----------
        paras : list of str, optional
            The parameters for which to do the actions.
            Can be one, some or all of ["n_d", "n", "t", "et"].
            The default is ["n_d", "n", "t", "et"].
        """
        log.info("="*79 + "\nBroker last_imp_fillup starts")
        self._check_paras(paras)
        for stations in self.stations:
            if stations._para in paras:
                stations.last_imp_fillup()

    def richter_correct(self):
        """Richter correct all of the precipitation data.
        """
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        self.stations_n.richter_correct()

    def last_imp_corr(self):
        """Richter correct the last imported precipitation data.
        """
        log.info("="*79 + "\nBroker: last_imp_corr starts")
        self.stations_n.last_imp_corr()

    def update_db(self, paras=["n_d", "n", "t", "et"]):
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
        """
        log.info("="*79 + "\nBroker update_db starts")
        self._check_paras(paras)
        self.check_is_broker_active()

        if pv.parse(__version__) > self.get_db_version():
            log.info("--> There is a new version of the python script. Therefor the database is recalculated completly")
            self.initiate_db()
        else:
            self.update_meta(paras=paras)
            self.update_raw(paras=paras)
            if "n_d" in paras:
                paras.remove("n_d")
            self.last_imp_quality_check(paras=paras)
            self.last_imp_fillup(paras=paras)
            self.last_imp_corr()
            self.set_is_broker_active(False)

    def initiate_db(self):
        """Initiate the Database.

        Downloads all the data from the CDC server for the first time.
        Updates the multi-annual data and the richter-class for all the stations.
        Quality checks and fills up the timeseries.
        """
        log.info("="*79 + "\nBroker initiate_db starts")
        self.check_is_broker_active()
        self.update_meta(
            paras=["n_d", "n", "t", "et"])
        self.update_raw(
            paras=["n_d", "n", "t", "et"],
            only_new=False)
        self.update_ma(
            paras=["n_d", "n", "t", "et"])
        self.stations_n.update_richter_class()
        self.quality_check(paras=["n", "t", "et"])
        self.fillup(paras=["n", "t", "et"])
        self.richter_correct()
        self.set_db_version()
        self.set_is_broker_active(False)

    def vacuum(self, do_analyze=True):
        sql = "VACUUM {anlyze};".format(
            analyze="ANALYZE" if do_analyze else "")
        with DB_ENG.connect() as con:
            con.execute(sqltxt(sql))

    def get_setting(self, key):
        """Get a specific settings value.

        Parameters
        ----------
        key : str
            The key of the setting.

        Returns
        -------
        value: str
            The version of the database.
        """
        with DB_ENG.connect() as con:
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
        with DB_ENG.connect() as con:
            con.execute(sqltxt(
                f"""INSERT INTO settings
                VALUES ('{key}', '{value}')
                ON CONFLICT (key)
                    DO UPDATE SET value=EXCLUDED.value;"""))

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