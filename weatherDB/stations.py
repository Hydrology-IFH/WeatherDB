# libraries
# from multiprocessing import queues
import warnings
import traceback
import pandas as pd
import geopandas as gpd
from shapely import wkt
import multiprocessing as mp
from multiprocessing.pool import ThreadPool
import time
import progressbar as pb
import logging
import itertools
import datetime
# from pathlib import Path
# import re
from tempfile import TemporaryDirectory

from .lib.connections import CDC, DB_ENG, check_superuser
from .lib.utils import get_ftp_file_list
from .lib.max_fun.import_DWD import get_dwd_meta
from .station import (StationBase,
    PrecipitationDailyStation,  PrecipitationStation,
    TemperatureStation, EvapotranspirationStation,
    GroupStation)

# set settings
try:# else I get strange errors with linux
    mp.set_start_method('spawn')
except RuntimeError:
    pass

# set log
#########
log = logging.getLogger(__name__)

# Base class definitions
########################

class StationsBase:
    _StationClass = StationBase
    _timeout_raw_imp = 240

    def __init__(self):
        if type(self) == StationsBase:
            raise NotImplementedError("""
            The StationsBase is only a wrapper class an is not working on its own.
            Please use PrecipitationStation, PrecipitationDailyStation, TemperatureStation or EvapotranspirationStation instead""")
        self._ftp_folder_base = self._StationClass._ftp_folder_base
        if type(self._ftp_folder_base) == str:
            self._ftp_folder_base = [self._ftp_folder_base]

        # create ftp_folders in order of importance
        self._ftp_folders = list(itertools.chain(*[
            [base + "historical/", base + "recent/"]
                for base in self._ftp_folder_base]))

        self._para = self._StationClass._para
        self._para_long = self._StationClass._para_long

    def download_meta(self):
        """Download the meta file(s) from the CDC server.

        Returns
        -------
        geopandas.GeoDataFrame
            The meta file from the CDC server.
            If there are several meta files on the server, they are joined together.
        """
        # download historic meta file
        meta = get_dwd_meta(self._ftp_folders[0])

        for ftp_folder in self._ftp_folders[1:]:
            meta_new = get_dwd_meta(ftp_folder=ftp_folder)

            # add new stations
            meta = meta.append(meta_new[~meta_new.index.isin(meta.index)])

            # check for wider timespan
            if "bis_datum" in meta.columns:
                meta = meta.join(
                    meta_new[["bis_datum", "von_datum"]],
                    how="left", rsuffix="_new")

                mask = meta["von_datum"] > meta["von_datum_new"]
                meta.loc[mask, "von_datum"] = meta_new.loc[mask, "von_datum"]

                mask = meta["bis_datum"] < meta["bis_datum_new"]
                meta.loc[mask, "bis_datum"] = meta_new.loc[mask, "bis_datum"]

                meta.drop(["von_datum_new", "bis_datum_new"], axis=1, inplace=True)

        return meta

    @check_superuser
    def update_meta(self):
        """Update the meta table by comparing to the CDC server.

        The "von_datum" and "bis_datum" is ignored because it is better to set this by the filled period of the stations in the database.
        Often the CDC period is not correct.
        """
        log.info(
            "The {para_long} meta table gets updated."\
                .format(para_long=self._para_long))
        meta = self.download_meta()

        meta["is_real"] = True

        # get droped stations and delete from meta file
        sql_get_droped = """
            SELECT station_id
            FROM droped_stations
            WHERE para ='{para}';
        """.format(para=self._para)

        # """
        #     SELECT split_part(station_id_para, '_', 1)::int as station_id
        #     FROM droped_stations
        #     WHERE station_id_para LIKE '%%\_{para}';
        # """.format(para=self._para)
        with DB_ENG.connect() as con:
            droped_stids = con.execute(sql_get_droped).all()
        droped_stids = [row[0] for row in droped_stids]
        meta.drop(droped_stids, inplace=True)

        # to have a meta entry for every station before looping over them
        if "von_datum" in meta.columns and "bis_datum" in meta.columns:
            self._update_db_meta(
                meta=meta.drop(["von_datum", "bis_datum"], axis=1))
        else:
            self._update_db_meta(meta=meta)
            # meta[["von_datum", "bis_datum"]] = pd.NaT

        # update von_datum and bis_datum
        # for stid in meta.index:
        #     try:
        #         stat = self._StationClass(stid)
        #     except NotImplementedError:
        #         continue
        #     period = stat.get_filled_period(kind="raw")
        #     if (period[0] is not None) and \
        #             (meta.loc[stid, "von_datum"] > pd.Timestamp(period[0])):
        #         meta.loc[stid, "von_datum"] = period[0]
        #     if (period[1] is not None) and \
        #             (meta.loc[stid, "bis_datum"] < pd.Timestamp(period[1])):
        #         meta.loc[stid, "bis_datum"] = period[1]

        # self._update_db_meta(
        #     meta=meta[["von_datum", "bis_datum"]])

        log.info(
            "The {para_long} meta table got successfully updated."\
                .format(para_long=self._para_long))

    @check_superuser
    def _update_db_meta(self, meta):
        """Update a meta table on the database with new DataFrame.

        Parameters
        ----------
        meta : pandas.DataFrame
            A DataFrame with stations_id as index.
        """
        # get the columns of meta
        meta = meta.rename_axis("station_id").reset_index()
        columns = [col.lower() for col in meta.columns]
        columns = columns + ['geometry_utm'] if 'geometry' in columns else columns
        meta.rename(dict(zip(meta.columns, columns)), axis=1, inplace=True)

        # check if columns are initiated in DB
        with DB_ENG.connect() as con:
            columns_db = con.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='meta_{para}';
                """.format(para=self._para)
                ).all()
            columns_db = [col[0] for col in columns_db]

        col_mask = [col in columns_db for col in columns]
        if False in col_mask:
            warnings.warn("""
                The meta_{para} column '{cols}' is not initiated in the database.
                This column is therefor skiped.
                Please review the DB or the code.
                """.format(
                    para=self._para,
                    cols=", ".join(
                        [col for col, mask in zip(columns, col_mask)
                             if not mask]))
                    )
            columns = columns[col_mask]

        # change date columns
        for colname, col in \
                meta.select_dtypes(include="datetime64").iteritems():
            meta.loc[:,colname] = col.dt.strftime("%Y%m%d %H:%M")

        # change geometry
        if "geometry" in meta.columns:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                meta["geometry_utm"] = meta.geometry.to_crs(25832).to_wkt()
                meta["geometry"] = meta.geometry.to_crs(4326).to_wkt()

        # change all to strings
        meta = meta.astype(str)

        # get values
        values_all = ["', '".join(pair) for pair in meta.loc[:,columns].values]
        values = "('" + "'), ('".join(values_all) + "')"
        values = values.replace("'nan'", "NULL").replace("'<NA>'", "NULL")

        # create sql
        sql = '''
            INSERT INTO meta_{para}({columns})
            Values {values}
            ON CONFLICT (station_id) DO UPDATE SET
            '''.format(
                columns=", ".join(columns),
                values=values,
                para=self._para)
        for col in columns:
            sql += ' "{col}" = EXCLUDED."{col}", '.format(col=col)

        sql = sql[:-2] + ";"

        # run sql command
        with DB_ENG.connect() as con:
            con.execute(sql)

    @check_superuser
    def update_period_meta(self, stids="all"):
        """Update the period in the meta table of the raw data.

        Parameters
        ----------
        stids: string  or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_simple_loop(
            stations=self.get_stations(only_real=True, stids=stids),
            methode="update_period_meta",
            name="update period in meta",
            kwargs={}
        )

    def get_meta(self,
            columns=["Station_id", "von_datum", "bis_datum", "geometry"],
            only_real=True):
        """Get the meta Dataframe from the Database.

        Parameters
        ----------
        columns : list, optional
            A list of columns from the meta file to return
            The default is: ["Station_id", "von_datum", "bis_datum", "geometry"]
        only_real: bool, optional
            Whether only real stations are returned or also virtual ones.
            True: only stations with own data are returned.
            The default is True.

        Returns
        -------
        pandas.DataFrame or geopandas.GeoDataFrae
            The meta DataFrame.
        """
        # make sure columns is of type list
        if type(columns) == str:
            columns = [columns]

        # check columns
        columns = [col.lower() for col in columns]
        if "station_id" not in columns:
            columns.insert(0, "station_id")
        if "geometry" in columns and "geometry_utm" in columns:
            warnings.warn("""
            You selected 2 geometry columns.
            Only the geometry column with EPSG 4326 is returned""")
            columns.remove("geometry_utm")

        # create geometry select statement
        columns_select = []
        for col in columns:
            if col in ["geometry", "geometry_utm"]:
                columns_select.append(
                    "ST_AsText({col}) as {col}".format(col=col))
            else:
                columns_select.append(col)

        # create sql statement
        sql = "SELECT {cols} FROM meta_{para}"\
            .format(cols=", ".join(columns_select), para=self._para)
        if only_real:
            sql += " WHERE is_real=true"

        # execute queries to db
        with DB_ENG.connect() as con:
            meta = pd.read_sql(
                sql, con,
                index_col="station_id",
                parse_dates=["von_datum", "bis_datum"])

        # change to GeoDataFrame if geometry column was selected
        for geom_col, srid in zip(["geometry", "geometry_utm"],
                                  ["4326",     "25832"]):
            if geom_col in columns:
                meta[geom_col] = meta[geom_col].apply(wkt.loads)
                meta = gpd.GeoDataFrame(meta, crs="EPSG:" + srid)

        return meta

    def get_stations(self, only_real=True, stids="all"):
        """Get a list with all the stations as Station-objects.

        Parameters
        ----------
        only_real: bool, optional
            Whether only real stations are returned or also virtual ones.
            True: only stations with own data are returned.
            The default is True.
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
        meta = self.get_meta(columns=["Station_id"], only_real=only_real)

        if stids == "all":
            stations = [self._StationClass(stid) for stid in meta.index]
        else:
            stids = list(stids)
            stations = [self._StationClass(stid) for stid in meta.index if stid in stids]
            stations_ids = [stat.id for stat in stations]
            if len(stations) != len(stids):
                raise ValueError(
                    "It was not possible to create a {para_long} Station with the following IDs: {stids}".format(
                        para_long=self._para_long,
                        stids = ", ".join([stid for stid in stids if stid in stations_ids])
                    ))

        return stations

    @staticmethod
    def _get_progressbar(max_value, name):
        pbar = pb.ProgressBar(
            widgets=[
                pb.widgets.RotatingMarker(),
                " " + name ,
                pb.widgets.Percentage(), ' ',
                pb.widgets.SimpleProgress(
                    format=("('%(value_s)s/%(max_value_s)s')")), ' ',
                pb.widgets.Bar(min_width=80), ' ',
                pb.widgets.Timer(format='%(elapsed)s'), ' | ',
                pb.widgets.ETA(),
                pb.widgets.DynamicMessage(
                    "last_station",
                    format=", last id: {formatted_value}",
                    precision=4)
                ],
            max_value=max_value,
            variables={"last_station": "None"},
            term_width=100,
            is_terminal=True
            )
        pbar.update(0)

        return pbar

    def _run_in_mp(self, stations, methode, name, kwargs=dict(), do_mp=True, processes=mp.cpu_count()-1):
        """Run methods of the given stations objects in multiprocessing/threading mode.

        Parameters
        ----------
        stations : list of station objects
            A list of station objects. Those must be children of the StationBase class.
        methode : str
            The name of the methode to call.
        name : str
            A descriptive name of the method to show in the progressbar.
        kwargs : dict
            The keyword arguments to give to the methodes
        do_mp : bool, optional
            Should the methode be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a methode is done in the postgresql database, then threading is enough to speed the process up.
            The default is True.
        """
        log.info("-"*79 +
        "\n{para_long} Stations async loop over methode '{methode}' started.".format(
            para_long=self._para_long,
            methode=methode
        ))
        # progressbar
        num_stations = len(stations)
        pbar = self._get_progressbar(max_value=num_stations, name=name)

        # create pool
        if do_mp:
            pool = mp.Pool(processes=processes)
        else:
            pool = ThreadPool(processes=processes)

        # start processes
        results = []
        for stat in stations:
            results.append(
                pool.apply_async(
                    getattr(stat, methode),
                    kwds=kwargs))
        pool.close()

        # check results until all finished
        finished = [False] * num_stations
        while (True):
            if all(finished): break

            for result in [result for i, result in enumerate(results)
                                  if not finished[i]]:
                if result.ready():
                    index = results.index(result)
                    finished[index] = True
                    pbar.variables["last_station"] = stations[index].id
                    # get stdout and log
                    try:
                        stdout = "stdout: " + str(result.get(10))
                    except:
                        stdout = "stderr: " + traceback.format_exc()
                    if stdout != "stdout: None":
                        log.debug((
                            "The {name} of the {para_long} Station " +
                            "with ID {stid} finished with:\n" +
                            "{stdout}"
                            ).format(
                                name=name,
                                para_long=self._para_long,
                                stid=stations[index].id,
                                stdout=stdout))

                    pbar.update(sum(finished))
            time.sleep(2)

        pool.join()
        pool.terminate()

    def _run_simple_loop(self, stations, methode, name, kwargs=dict()):
        log.info("-"*79 +
        "\n{para_long} Stations simple loop over methode '{methode}' started.".format(
            para_long=self._para_long,
            methode=methode
        ))

        # progressbar
        num_stations = len(stations)
        pbar = self._get_progressbar(max_value=num_stations, name=name)

        # start processes
        results = []
        for stat in stations:
            getattr(stat, methode)(**kwargs)
            pbar.variables["last_station"] = stat.id
            pbar.update()

    @check_superuser
    def update_raw(self, only_new=True, only_real=True, stids="all"):
        """Download all stations data from CDC and upload to database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True
        only_real: bool, optional
            Whether only real stations are tried to download.
            True: only stations with a date in von_datum in meta are downloaded.
            The default is True.
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        start_tstp = datetime.datetime.now()

        # get FTP file list
        CDC.login()
        ftp_file_list = get_ftp_file_list(
            ftp_conn=CDC,
            ftp_folders=self._ftp_folders)

        # run the tasks in multiprocessing mode
        self._run_in_mp(
            stations=self.get_stations(only_real=only_real, stids=stids),
            methode="update_raw",
            name="download raw {para} data".format(para=self._para.upper()),
            kwargs=dict(
                only_new=only_new,
                ftp_file_list=ftp_file_list),
            do_mp=True)

        # save start time as variable to db
        with DB_ENG.connect() as con:
            con.execute("""
                UPDATE para_variables
                SET start_tstp_last_imp='{start_tstp}'::timestamp
                WHERE para='{para}';
            """.format(
                para=self._para,
                start_tstp=start_tstp.strftime("%Y%m%d %H:%M")))

    @check_superuser
    def last_imp_quality_check(self):
        """Do the quality check of the last import.
        """
        self._run_in_mp(
            stations=self.get_stations(only_real=True),
            methode="last_imp_quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            kwargs=dict(period=(None,None)),
            do_mp=False)

    @check_superuser
    def last_imp_fillup(self):
        """Do the filling of the last import.
        """
        stations = self.get_stations(only_real=True)
        period = stations[0].get_last_imp_period(all=True)
        log.info("The {para_long} Stations fillup of the last import is started for the period {min_tstp} - {max_tstp}".format(
            para_long=self._para_long,
            min_tstp=period[0].strftime("%Y%m%d %H:%M"),
            max_tstp=period[1].strftime("%Y%m%d %H:%M")))
        self._run_in_mp(
            stations=stations,
            methode="last_imp_fillup",
            name="fillup {para} data".format(para=self._para.upper()),
            kwargs=dict(period=period),
            do_mp=False)

    @check_superuser
    def quality_check(self, period=(None, None), only_real=True, stids="all"):
        """Quality check the raw data for a given period.

        Parameters
        ----------
        period : tuple or list of datetime.datetime or None, optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        """
        self._run_in_mp(
            stations=self.get_stations(only_real=only_real),
            methode="quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            kwargs=dict(period=period),
            do_mp=False)

    @check_superuser
    def update_ma(self, stids="all"):
        """Update the multi annual values for the stations.

        Get a multi annual value from the corresponding raster and save to the multi annual table in the database.

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
            methode="update_ma",
            name="update ma-values for {para}".format(para=self._para.upper()),
            kwargs=dict(),
            do_mp=False)

    @check_superuser
    def fillup(self, only_real=False, stids="all"):
        """Fill up the quality checked data with data from nearby stations to get complete timeseries.

        Parameters
        ----------
        only_real: bool, optional
            Whether only real stations are computed or also virtual ones.
            True: only stations with own data are returned.
            The default is True.
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
            stations=self.get_stations(only_real=only_real, stids=stids),
            methode="fillup",
            kwargs={},
            name="fillup {para} data".format(para=self._para.upper()),
            do_mp=False)

class StationsTETBase(StationsBase):
    @check_superuser
    def fillup(self, only_real=False, stids="all"):
        # create virtual stations if necessary
        if not only_real:
            meta = self.get_meta(
                columns=["Station_id"], only_real=False)
            meta_n = StationsN().get_meta(
                columns=["Station_id"], only_real=False)
            stids_missing = set(meta_n.index.values) - set(meta.index.values)
            if stids != "all":
                stids_missing = set(stids).intersection(stids_missing)
            for stid in stids_missing:
                self._StationClass(stid) # this creates the virtual station

        super().fillup(only_real=only_real,stids=stids)

# Implement stations classes
class PrecipitationStations(StationsBase):
    _StationClass = PrecipitationStation
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


class PrecipitationDailyStations(StationsBase):
    _StationClass = PrecipitationDailyStation
    _StationClass_parent = PrecipitationStations
    _timeout_raw_imp = 120

    # def download_meta(self):
    #     meta = super().download_meta()
    #     # stids_10min = self._StationClass_parent().get_meta(columns=["station_id"]).index
    #     # meta = meta[meta.index.isin(stids_10min)]
    #     return meta


class TemperatureStations(StationsTETBase):
    _StationClass = TemperatureStation
    _timeout_raw_imp = 120


class EvapotranspirationStations(StationsTETBase):
    _StationClass = EvapotranspirationStation
    _timeout_raw_imp = 120

    # def download_raw(self, only_new=True, only_real=False):
    #     # because the evapotranspiration meta file from CDC has no date collumn
    #     # the default value for only_real is therefor changed to try
    #     # to download all the stations in the meta table
    #     return super().download_raw(only_new=only_new, only_real=only_real)


# shorter names for classes
StationsN = PrecipitationStations
StationsND = PrecipitationDailyStations
StationsT = TemperatureStations
StationsET = EvapotranspirationStations


# create a grouping class
class GroupStations(object):
    """A class to group all possible parameters of all the stations.
    """
    _StationN = PrecipitationStation
    _GroupStation = GroupStation
    _StationsBase = StationsBase

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
        only_real: bool, optional
            Whether only real stations are returned or also virtual ones.
            True: only stations with own data are returned.
            The default is True.

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

    def create_roger_ts(self, dir, period=(None, None), kind="best", stids="all",):
        """Get a list with all the stations as Station-objects.

        Parameters
        ----------
        only_real: bool, optional
            Whether only real stations are returned or also virtual ones.
            True: only stations with own data are returned.
            The default is True.
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
        # check directory
        dir = self._GroupStation._check_dir(dir)

        # create GroupStation instances
        stats = self.get_stations(stids=stids)
        pbar = self._StationsBase._get_progressbar(
            max_value=len(stats),
            name="create RoGeR-TS")
        pbar.update(0)
        for stat in stats:
            stat.create_roger_ts(
                dir=dir.joinpath(str(stat.id)),
                period=period,
                kind=kind)
            pbar.variables["last_station"] = stat.id
            pbar.update(pbar.value + 1)

    def create_roger_ts_tempzip(self, period=(None, None), kind="best", stids="all"):
        temp_dir = TemporaryDirectory()

    def _check_valid_stids(self, stids):
        meta = self.get_meta(columns=["Station_id"])
        if all([stid in meta.index for stid in stids]):
            return True
        else:
            return False

# clean station
del PrecipitationStation, PrecipitationDailyStation, TemperatureStation, EvapotranspirationStation, GroupStation