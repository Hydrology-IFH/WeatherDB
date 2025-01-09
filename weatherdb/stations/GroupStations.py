# libraries
import warnings
import pandas as pd
import geopandas as gpd
import logging
import datetime
import socket
import zipfile
from pathlib import Path
from sqlalchemy import text as sqltxt

from ..db.connections import db_engine
from ..utils import TimestampPeriod
from ..station import StationP, GroupStation
from .StationsP import StationsP
from .StationsT import StationsT
from .StationsET import StationsET
from .StationsBase import StationsBase

# set settings
# ############
__all__ = ["GroupStations"]
log = logging.getLogger(__name__)

# class definition
##################

class GroupStations(object):
    """A class to group all possible parameters of all the stations.
    """
    _StationP = StationP
    _GroupStation = GroupStation

    def __init__(self):
        self.stationsN = StationsP()

    def get_valid_stids(self):
        if not hasattr(self, "_valid_stids"):
            sql ="""SELECT station_id FROM meta_p"""
            with db_engine.connect() as con:
                res = con.execute(sqltxt(sql))
            self._valid_stids = [el[0] for el in res.all()]
        return self._valid_stids

    def _check_paras(self, paras):
        if isinstance(paras, str) and paras != "all":
            paras = [paras,]

        valid_paras=["n", "t", "et"]
        if isinstance(paras, str) and (paras == "all"):
            return valid_paras
        else:
            paras_new = []
            for para in paras:
                if para in valid_paras:
                    paras_new.append(para)
                else:
                    raise ValueError(
                        f"The parameter {para} you asked for is not a valid parameter. Please enter one of {valid_paras}")
            return paras_new

    def _check_period(self, period, stids, kinds, nas_allowed=True):
        # get max_period of stations
        max_period = None
        for stid in stids:
            max_period_i = self._GroupStation(stid).get_max_period(
                kinds=kinds, nas_allowed=nas_allowed)
            if max_period is not None:
                max_period = max_period.union(
                    max_period_i,
                    how="outer" if nas_allowed else "inner"
                )
            else:
                max_period=max_period_i

        if not isinstance(period, TimestampPeriod):
            period = TimestampPeriod(*period)
        if period.is_empty():
            return max_period
        else:
            if not period.inside(max_period):
                period = period.union(max_period, how="inner")
                warnings.warn("The asked period is too large. Only {min_tstp} - {max_tstp} is returned".format(
                    **period.get_sql_format_dict(format="%Y-%m-%d %H:%M")))
            return period

    def _check_stids(self, stids):
        """Check if the given stids are valid Station IDs.

        It checks against the Precipitation stations.
        """
        if isinstance(stids, str) and (stids == "all"):
            return self.get_valid_stids()
        else:
            valid_stids = self.get_valid_stids()
            mask_stids_valid = [stid in valid_stids for stid in stids]
            if all(mask_stids_valid):
                return stids
            else:
                raise ValueError(
                    "There is no station defined in the database for the IDs: {stids}".format(
                        stids=", ".join(
                            [str(stid) for stid, valid in zip(stids, mask_stids_valid)
                                  if not valid])))

    @staticmethod
    def _check_dir(dir):
        """Checks if a directors is valid and empty.

        If not existing the directory is created.

        Parameters
        ----------
        dir : pathlib object or zipfile.ZipFile
            The directory to check.

        Raises
        ------
        ValueError
            If the directory is not empty.
        ValueError
            If the directory is not valid. E.G. it is a file path.
        """
        # check types
        if isinstance(dir, str):
            dir = Path(dir)

        # check directory
        if isinstance(dir, zipfile.ZipFile):
            return dir
        elif isinstance(dir, Path):
            if dir.is_dir():
                if len(list(dir.iterdir())) > 0:
                    raise ValueError(
                        "The given directory '{dir}' is not empty.".format(
                            dir=str(dir)))
            elif dir.suffix == "":
                dir.mkdir()
            elif dir.suffix == ".zip":
                if not dir.parent.is_dir():
                    raise ValueError(
                        "The given parent directory '{dir}' of the zipfile is not a directory.".format(
                            dir=dir.parents))
            else:
                raise ValueError(
                    "The given directory '{dir}' is not a directory.".format(
                        dir=dir))
        else:
            raise ValueError(
                "The given directory '{dir}' is not a directory or zipfile.".format(
                    dir=dir))

        return dir

    @classmethod
    def get_meta_explanation(cls, infos="all"):
        """Get the explanations of the available meta fields.

        Parameters
        ----------
        infos : list or string, optional
            The infos you wish to get an explanation for.
            If "all" then all the available information get returned.
            The default is "all"

        Returns
        -------
        pd.Series
            a pandas Series with the information names as index and the explanation as values.
        """
        return cls._GroupStation.get_meta_explanation(infos=infos)

    def get_meta(self, paras="all", stids="all", **kwargs):
        """Get the meta Dataframe from the Database.

        Parameters
        ----------
        paras : list or str, optional
            The parameters for which to get the information.
            If "all" then all the available parameters are requested.
            The default is "all".
        stids: string or list of int, optional
            The Stations to return the meta information for.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs: dict, optional
            The keyword arguments are passed to the station.GroupStation().get_meta method.
            From there it is passed to the single station get_meta method.
            Can be e.g. "infos"

        Returns
        -------
        dict of pandas.DataFrame or geopandas.GeoDataFrame
        or pandas.DataFrame or geopandas.GeoDataFrame
            The meta DataFrame.
            If several parameters are asked for, then a dict with an entry per parameter is returned.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        ValueError
            If the given paras are not all valid.
        """
        paras = self._check_paras(paras)
        stats = self.get_para_stations(paras=paras)

        for stat in stats:
            meta_para = stat.get_meta(stids=stids, **kwargs)
            meta_para["parameter"] = stat._para
            if "meta_all" not in locals():
                meta_all = meta_para
            else:
                meta_all = pd.concat([meta_all, meta_para], axis=0)
                if isinstance(meta_para, gpd.GeoDataFrame):
                    meta_all = gpd.GeoDataFrame(meta_all, crs=meta_para.crs)

        if len(paras)==1:
            return meta_all.drop("parameter", axis=1)
        else:
            return meta_all.reset_index().set_index(["station_id", "parameter"]).sort_index()

    def get_para_stations(self, paras="all"):
        """Get a list with all the multi parameter stations as stations.Station*Parameter*-objects.

        Parameters
        ----------
        paras : list or str, optional
            The parameters for which to get the objects.
            If "all" then all the available parameters are requested.
            The default is "all".

        Returns
        -------
        Station-object
            returns a list with the corresponding station objects.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        paras = self._check_paras(paras)
        if not hasattr(self, "stations"):
            self.stations = [self.stationsN, StationsT(), StationsET()]
        return [stats for stats in self.stations if stats._para in paras]

    def get_group_stations(self, stids="all", **kwargs):
        """Get a list with all the stations as station.GroupStation-objects.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs: optional
            The keyword arguments are handed to the creation of the single GroupStation objects.
            Can be e.g. "error_if_missing".

        Returns
        -------
        Station-object
            returns a list with the corresponding station objects.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        if "error_if_missing" not in kwargs:
            kwargs.update({"error_if_missing": False})
        kwargs.update({"_skip_meta_check":True})
        valid_stids = self.get_valid_stids()

        if isinstance(stids, str) and (stids == "all"):
            stations = [
                self._GroupStation(stid, **kwargs)
                for stid in valid_stids]
        else:
            stids = list(stids)
            stations = [self._GroupStation(stid, **kwargs)
                        for stid in valid_stids if stid in stids]
            stations_ids = [stat.id for stat in stations]
            if len(stations) != len(stids):
                raise ValueError(
                    "It was not possible to create a {para_long} Station with the following IDs: {stids}".format(
                        para_long=self._para_long,
                        stids = ", ".join([stid for stid in stids if stid in stations_ids])
                    ))

        return stations

    def create_ts(self, dir, period=(None, None), kinds="best",
                  stids="all", agg_to="10 min", r_r0=None, split_date=False,
                  nas_allowed=True, add_na_share=False,
                  add_t_min=False, add_t_max=False,
                  **kwargs):
        """Download and create the weather tables as csv files.

        Parameters
        ----------
        dir : path-like object
            The directory where to save the tables.
            If the directory is a ZipFile, then the output will get zipped into this.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kinds :  str or list of str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        stids : string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".
        r_r0 : int or float or None or pd.Series or list, optional
            Should the ET timeserie contain a column with R/R0.
            If None, then no column is added.
            If int, then a R/R0 column is appended with this number as standard value.
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appended to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The series is then joined to the ET timeserie.
            The default is None.
        split_date : bool, optional
            Should the timestamp get splitted into parts, so one column for year, one for month etc.?
            If False the timestamp is saved in one column as string.
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is True.
        add_na_share : bool, optional
            Should one or several columns be added to the Dataframe with the share of NAs in the data.
            This is especially important, when the stations data get aggregated, because the aggregation doesn't make sense if there are a lot of NAs in the original data.
            If True, one column per asked kind is added with the respective share of NAs, if the aggregation step is not the smallest.
            The "kind"_na_share column is in percentage.
            The default is False.
        add_t_min : bool, optional
            Should the minimal temperature value get added?
            The default is False.
        add_t_max : bool, optional
            Should the maximal temperature value get added?
            The default is False.
        **kwargs:
            additional parameters for GroupStation.create_ts
        """
        start_time = datetime.datetime.now()
        # check directory and stids
        dir = self._check_dir(dir)
        stids = self._check_stids(stids)

        # check period
        period = self._check_period(
            period=period, stids=stids, kinds=kinds,
            nas_allowed=nas_allowed)
        if period.is_empty():
            raise ValueError("For the given settings, no timeseries could get extracted from the database.\nMaybe try to change the nas_allowed parameter to True, to see, where the problem comes from.")

        # create GroupStation instances
        gstats = self.get_group_stations(stids=stids)
        pbar = StationsBase._get_progressbar(
            max_value=len(gstats),
            name="create TS")
        pbar.update(0)

        if dir.suffix == ".zip":
            with zipfile.ZipFile(
                    dir, "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=5) as zf:
                for stat in gstats:
                    stat.create_ts(
                        dir=zf,
                        period=period,
                        kinds=kinds,
                        agg_to=agg_to,
                        r_r0=r_r0,
                        split_date=split_date,
                        nas_allowed=nas_allowed,
                        add_na_share=add_na_share,
                        add_t_min=add_t_min,
                        add_t_max=add_t_max,
                        _skip_period_check=True,
                        **kwargs)
                    pbar.variables["last_station"] = stat.id
                    pbar.update(pbar.value + 1)
        else:
            for stat in gstats:
                stat.create_ts(
                    dir=dir.joinpath(str(stat.id)),
                    period=period,
                    kinds=kinds,
                    agg_to=agg_to,
                    r_r0=r_r0,
                    split_date=split_date,
                    nas_allowed=nas_allowed,
                    add_na_share=add_na_share,
                    add_t_min=add_t_min,
                    add_t_max=add_t_max,
                    **kwargs)
                pbar.variables["last_station"] = stat.id
                pbar.update(pbar.value + 1)

        # get size of output file
        if dir.suffix == ".zip":
            out_size = dir.stat().st_size
        else:
            out_size = sum(
                f.stat().st_size for f in dir.glob('**/*') if f.is_file())

        # save needed time to db
        if db_engine.is_superuser:
            sql_save_time = """
                INSERT INTO needed_download_time(timestamp, quantity, aggregate, timespan, zip, pc, duration, output_size)
                VALUES (now(), '{quantity}', '{agg_to}', '{timespan}', '{zip}', '{pc}', '{duration}', '{out_size}');
            """.format(
                quantity=len(stids),
                agg_to=agg_to,
                timespan=str(period.get_interval()),
                duration=str(datetime.datetime.now() - start_time),
                zip="true" if dir.suffix ==".zip" else "false",
                pc=socket.gethostname(),
                out_size=out_size)
            with db_engine.connect() as con:
                con.execute(sqltxt(sql_save_time))
                con.commit()

        # create log message
        log.debug(
            "The timeseries tables for {quantity} stations got created in {dir}".format(
                quantity=len(stids), dir=dir))

    def create_roger_ts(self, dir, period=(None, None), stids="all",
                        kind="best", r_r0=1,
                        add_t_min=False, add_t_max=False,
                        do_toolbox_format=False, **kwargs):
        """Create the timeserie files for roger as csv.

        This is only a wrapper function for create_ts with some standard settings.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the stations ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        r_r0 : int or float or None or pd.Series or list, optional
            Should the ET timeserie contain a column with R_R0.
            If None, then no column is added.
            If int, then a R/R0 column is appended with this number as standard value.
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appended to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The series is then joined to the ET timeserie.
            The default is 1.
        add_t_min : bool, optional
            Should the minimal temperature value get added?
            The default is False.
        add_t_max : bool, optional
            Should the maximal temperature value get added?
            The default is False.
        do_toolbox_format : bool, optional
            Should the timeseries be saved in the RoGeR toolbox format? (have a look at the RoGeR examples in https://github.com/Hydrology-IFH/roger)
            The default is False.
        **kwargs:
            additional parameters for GroupStation.create_ts

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        if do_toolbox_format:
            return self.create_ts(
                dir=dir, period=period, kinds=kind,
                agg_to="10 min", r_r0=r_r0, stids=stids,
                split_date=True, nas_allowed=False,
                add_t_min=add_t_min, add_t_max=add_t_max,
                file_names={"N":"PREC.txt", "T":"TA.txt", "ET":"PET.txt"},
                col_names={"N":"PREC", "ET":"PET",
                           "T":"TA", "T_min":"TA_min", "T_max":"TA_max",
                           "Jahr":"YYYY", "Monat":"MM", "Tag":"DD",
                           "Stunde":"hh", "Minute":"mm"},
                add_meta=False,
                keep_date_parts=True,
                **kwargs)
        else:
            return self.create_ts(
                dir=dir, period=period, kinds=kind,
                agg_to="10 min", r_r0=r_r0, stids=stids,
                split_date=True, nas_allowed=False,
                add_t_min=add_t_min, add_t_max=add_t_max,
                **kwargs)
