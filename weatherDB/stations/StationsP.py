# libraries
import logging

from ..db.connections import db_engine
from ..station import StationP
from .StationsBase import StationsBase

# set settings
# ############
__all__ = ["StationsP"]
log = logging.getLogger(__name__)

# class definition
##################
class StationsP(StationsBase):
    """A class to work with and download 10 minutes precipitation data for several stations."""
    _StationClass = StationP
    _timeout_raw_imp = 360

    @db_engine.deco_update_privilege
    def update_richter_class(self, stids="all", do_mp=True, **kwargs):
        """Update the Richter exposition class.

        Get the value from the raster, compare with the richter categories and save to the database.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs : dict, optional
            The keyword arguments to be handed to the station.StationP.update_richter_class and get_stations method.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=True, stids=stids, **kwargs),
            method="update_richter_class",
            name="update richter class for {para}".format(para=self._para.upper()),
            kwds=kwargs,
            do_mp=do_mp)

    @db_engine.deco_update_privilege
    def richter_correct(self, stids="all", **kwargs):
        """Richter correct the filled data.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=False, stids=stids, **kwargs),
            method="richter_correct",
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=False, **kwargs)

    @db_engine.deco_update_privilege
    def last_imp_corr(self, stids="all", do_mp=False, **kwargs):
        """Richter correct the filled data for the last imported period.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is False.
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        stations = self.get_stations(only_real=True, stids=stids, **kwargs)
        period = stations[0].get_last_imp_period(all=True)
        log.info("The {para_long} Stations fillup of the last import is started for the period {min_tstp} - {max_tstp}".format(
            para_long=self._para_long,
            **period.get_sql_format_dict(format="%Y%m%d %H:%M")))
        self._run_method(
            stations=stations,
            method="last_imp_corr",
            kwds={"_last_imp_period": period},
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)

    @db_engine.deco_update_privilege
    def update(self, only_new=True, **kwargs):
        """Make a complete update of the stations.

        Does the update_raw, quality check, fillup and richter correction of the stations.

        Parameters
        ----------
        only_new : bool, optional
            Should a only new values be computed?
            If False: The stations are updated for the whole possible period.
            If True, the stations are only updated for new values.
            The default is True.
        """
        super().update(only_new=only_new, **kwargs)
        if only_new:
            self.last_imp_richter_correct(**kwargs)
        else:
            self.richter_correct(**kwargs)
