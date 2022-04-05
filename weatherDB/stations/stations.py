"""
This module has grouping classes for all the stations of one parameter. E.G. StationsN (or StationsN) groups all the Precipitation Stations available.
Those classes can get used to do actions on all the stations.
"""
# libraries
# from multiprocessing import queues
import warnings
import multiprocessing as mp
import logging
import datetime
import socket
import zipfile
from pathlib import Path

from ..lib.connections import DB_ENG, check_superuser
from ..lib.utils import  TimestampPeriod
from ..station import (
    StationND,  StationN,
    StationT, StationET,
    GroupStation)
from . import base

# set settings
try:# else I get strange errors with linux
    mp.set_start_method('spawn')
except RuntimeError:
    pass

# set log
#########
log = logging.getLogger(__name__)

# Implement stations classes
class StationsN(base.StationsBase):
    _StationClass = StationN
    _timeout_raw_imp = 360

    @check_superuser
    def update_richter_class(self, stids="all"):
        """Update the Richter exposition class.

        Get the value from the raster, compare with the richter categories and save to the database.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_in_mp(
            stations=self.get_stations(only_real=True, stids=stids),
            methode="update_richter_class",
            name="update richter class for {para}".format(para=self._para.upper()),
            kwargs=dict(),
            do_mp=False)

    @check_superuser
    def richter_correct(self, stids="all"):
        """Richter correct the filled data.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_in_mp(
            stations=self.get_stations(only_real=False, stids=stids),
            methode="richter_correct",
            kwargs={},
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=False)

    @check_superuser
    def last_imp_corr(self, stids="all"):
        """Richter correct the filled data for the last imported period.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        stations = self.get_stations(only_real=True, stids=stids)
        period = stations[0].get_last_imp_period(all=True)
        log.info("The {para_long} Stations fillup of the last import is started for the period {min_tstp} - {max_tstp}".format(
            para_long=self._para_long,
            **period.get_sql_format_dict(format="%Y%m%d %H:%M")))
        self._run_in_mp(
            stations=stations,
            methode="last_imp_corr",
            kwargs={"_last_imp_period": period},
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=False)


class StationsND(base.StationsBase):
    _StationClass = StationND
    _StationClass_parent = StationsN
    _timeout_raw_imp = 120

    # def download_meta(self):
    #     meta = super().download_meta()
    #     # stids_10min = self._StationClass_parent().get_meta(columns=["station_id"]).index
    #     # meta = meta[meta.index.isin(stids_10min)]
    #     return meta


class StationsT(base.StationsTETBase):
    _StationClass = StationT
    _timeout_raw_imp = 120


class StationsET(base.StationsTETBase):
    _StationClass = StationET
    _timeout_raw_imp = 120

    # def download_raw(self, only_new=True, only_real=False):
    #     # because the evapotranspiration meta file from CDC has no date column
    #     # the default value for only_real is therefor changed to try
    #     # to download all the stations in the meta table
    #     return super().download_raw(only_new=only_new, only_real=only_real)


# create a grouping class
class GroupStations(object):
    """A class to group all possible parameters of all the stations.

    So if you want to create the input files for a simulation, where you need T, ET and N, use this class to download the data.
    """
    _StationN = StationN
    _GroupStation = GroupStation

    def __init__(self):
        self.stationsN = StationsN()

    def get_meta(self,
            columns=["Station_id", "von_datum", "bis_datum", "geometry"]):
        """Get the meta Dataframe from the Database.

        Parameters
        ----------
        columns : list, optional
            A list of columns from the meta file to return
            The default is: ["Station_id", "von_datum", "bis_datum", "geometry"]

        Returns
        -------
        pandas.DataFrame or geopandas.GeoDataFrae
            The meta DataFrame.
        """
        return self.stationsN.get_meta(columns=columns, only_real=True)

    def get_stations(self, stids="all"):
        """Get a list with all the stations as Station-objects.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
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
        meta = self.get_meta(columns=["Station_id"])

        if stids == "all":
            stations = [self._GroupStation(stid) for stid in meta.index]
        else:
            stids = list(stids)
            stations = [self._GroupStation(stid) for stid in meta.index if stid in stids]
            stations_ids = [stat.id for stat in stations]
            if len(stations) != len(stids):
                raise ValueError(
                    "It was not possible to create a {para_long} Station with the following IDs: {stids}".format(
                        para_long=self._para_long,
                        stids = ", ".join([stid for stid in stids if stid in stations_ids])
                    ))

        return stations

    def create_ts(self, dir, period=(None, None), kind="best",
                  stids="all", agg_to="10 min", et_et0=None, split_date=False):
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
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        stids: string or list of int, optional
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
        et_et0 : int or None, optional
            Should the ET timeserie contain a column with et_et0.
            If None, then no column is added.
            If int, then a ET/ET0 column is appended with this number as standard value.
            Until now providing a serie of different values is not possible.
            The default is None.
        split_date : bool, optional
            Should the timestamp get splitted into parts, so one column for year, one for month etc.?
            If False the timestamp is saved in one column as string.
        """
        start_time = datetime.datetime.now()
        # check directory and stids
        dir = self._check_dir(dir)
        stids = self._check_stids(stids)

        # check period
        period = self._check_period(
            period=period, stids=stids, kind=kind)

        # create GroupStation instances
        stats = self.get_stations(stids=stids)
        pbar = base.StationsBase._get_progressbar(
            max_value=len(stats),
            name="create RoGeR-TS")
        pbar.update(0)

        if dir.suffix == ".zip":
            with zipfile.ZipFile(
                    dir, "w",
                    compression=zipfile.ZIP_DEFLATED,
                    compresslevel=5) as zf:
                for stat in stats:
                    stat.create_ts(
                        dir=zf,
                        period=period,
                        kind=kind,
                        agg_to=agg_to,
                        et_et0=et_et0,
                        split_date=split_date)
                    pbar.variables["last_station"] = stat.id
                    pbar.update(pbar.value + 1)
        else:
            for stat in stats:
                stat.create_ts(
                    dir=dir.joinpath(str(stat.id)),
                    period=period,
                    kind=kind,
                    agg_to=agg_to,
                    et_et0=et_et0,
                    split_date=split_date)
                pbar.variables["last_station"] = stat.id
                pbar.update(pbar.value + 1)

        # get size of output file
        if dir.suffix == ".zip":
            out_size = dir.stat().st_size
        else:
            out_size = sum(
                f.stat().st_size for f in dir.glob('**/*') if f.is_file())

        # save needed time to db
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
        with DB_ENG.connect() as con:
            con.execute(sql_save_time)

        # create log message
        log.debug(
            "The timeseries tables for {quantity} stations got created in {dir}".format(
                quantity=len(stids), dir=dir))

    def create_roger_ts(self, dir, period=(None, None),
                        kind="best", et_et0=1):
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
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        et_et0 : int or None, optional
            Should the ET timeserie contain a column with et_et0.
            If None, then no column is added.
            If int, then a ET/ET0 column is appended with this number as standard value.
            Until now providing a serie of different values is not possible.
            The default is 1.

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        return self.create_ts(dir=dir, period=period, kind=kind,
                              agg_to="10 min", et_et0=et_et0,
                              split_dates=True)

    def _check_period(self, period, stids, kind):
        max_period = self._GroupStation(stids[0]).get_filled_period(kind=kind)
        for stid in stids[1:]:
            max_period = max_period.union(
                self._GroupStation(stid).get_filled_period(kind=kind))

        if type(period) != TimestampPeriod:
            period = TimestampPeriod(*period)
        if period.is_empty():
            return max_period
        else:
            if not period.inside(max_period):
                warnings.warn("The asked period is too large. Only {min_tstp} - {max_tstp} is returned".format(
                    **max_period.get_sql_format_dict(format="%Y-%m-%d %H:%M")))
            return period.union(max_period)

    def _check_stids(self, stids):
        meta = self.get_meta(columns=["Station_id"])
        if stids == "all":
            return meta["Station_id"].values.to_list()
        else:
            stids_valid = [stid in meta.index for stid in stids]
            if all(stids_valid):
                return stids
            else:
                raise ValueError(
                    "There is no station defined in the database for the IDs:\n{stids}".format(
                        stids=", ".join(
                            [stid for stid in stids
                                  if stid not in stids_valid])))

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
        if type(dir) == str:
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

# clean station
del StationN, StationND, StationT, StationET, GroupStation