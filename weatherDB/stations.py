"""
This module has grouping classes for all the stations of one parameter. E.G. StationsN (or StationsN) groups all the Precipitation Stations available.
Those classes can get used to do actions on all the stations.
"""
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
import socket
import zipfile
from pathlib import Path
from sqlalchemy import text as sqltxt

from .lib.connections import DB_ENG, check_superuser
from .lib.utils import TimestampPeriod, get_cdc_file_list
from .lib.max_fun.import_DWD import get_dwd_meta
from .station import (StationBase,
    StationND,  StationN,
    StationT, StationET,
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
            Please use StationN, StationND, StationT or StationET instead""")
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
            meta = pd.concat(
                [meta, meta_new[~meta_new.index.isin(meta.index)]])
            if type(meta_new)==gpd.GeoDataFrame:
                meta = gpd.GeoDataFrame(meta, crs=meta_new.crs)

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

        # get droped stations and delete from meta file
        sql_get_droped = """
            SELECT station_id
            FROM droped_stations
            WHERE para ='{para}';
        """.format(para=self._para)
        with DB_ENG.connect() as con:
            droped_stids = con.execute(sqltxt(sql_get_droped)).all()
        droped_stids = [row[0] for row in droped_stids
                        if row[0] in meta.index]
        meta.drop(droped_stids, inplace=True)

        # to have a meta entry for every station before looping over them
        if "von_datum" in meta.columns and "bis_datum" in meta.columns:
            self._update_db_meta(
                meta=meta.drop(["von_datum", "bis_datum"], axis=1))
        else:
            self._update_db_meta(meta=meta)

        log.info(
            "The {para_long} meta table got successfully updated."\
                .format(para_long=self._para_long))

    @check_superuser
    def _update_db_meta(self, meta):
        """Update a meta table on the database with new DataFrame.

        Parameters
        ----------
        meta : pandas.DataFrame
            A DataFrame with station_id as index.
        """
        # get the columns of meta
        meta = meta.rename_axis("station_id").reset_index()
        columns = [col.lower() for col in meta.columns]
        columns = columns + ['geometry_utm'] if 'geometry' in columns else columns
        meta.rename(dict(zip(meta.columns, columns)), axis=1, inplace=True)

        # check if columns are initiated in DB
        with DB_ENG.connect() as con:
            columns_db = con.execute(sqltxt(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='meta_{para}';
                """.format(para=self._para)
                )).all()
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
                meta.select_dtypes(include="datetime64").items():
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
            con.execute(sqltxt(sql))

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
            method="update_period_meta",
            name="update period in meta",
            kwargs={}
        )

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
        return cls._StationClass.get_meta_explanation(infos=infos)

    def get_meta(self,
            infos=["station_id", "filled_from", "filled_until", "geometry"],
            stids="all",
            only_real=True):
        """Get the meta Dataframe from the Database.

        Parameters
        ----------
        infos : list or str, optional
            A list of information from the meta file to return
            If "all" than all possible columns are returned, but only one geometry column.
            The default is: ["Station_id", "filled_from", "filled_until", "geometry"]
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
        if type(infos) == str:
            if infos=="all":
                infos = self.get_meta_explanation(infos="all").index.to_list()
                if "geometry_utm" in infos:
                    infos.remove("geometry_utm")
            else:
                infos = [infos]

        # check infos
        infos = [col.lower() for col in infos]
        if "station_id" not in infos:
            infos.insert(0, "station_id")
        if "geometry" in infos and "geometry_utm" in infos:
            warnings.warn("""
            You selected 2 geometry columns.
            Only the geometry column with EPSG 4326 is returned""")
            infos.remove("geometry_utm")

        # create geometry select statement
        infos_select = []
        for info in infos:
            if info in ["geometry", "geometry_utm"]:
                infos_select.append(
                    f"ST_AsText({info}) as {info}")
            else:
                infos_select.append(info)

        # create sql statement
        sql = "SELECT {cols} FROM meta_{para}"\
            .format(cols=", ".join(infos_select), para=self._para)
        if only_real:
            where_clause = " WHERE is_real=true"
        if stids != "all":
            if type(stids) != list:
                stids = [stids,]
            if "where_clause" not in locals():
                where_clause = " WHERE "
            else:
                where_clause += " AND "
            where_clause += "station_id in ({stids})".format(
                stids=", ".join([str(stid) for stid in stids]))
        if "where_clause" in locals():
            sql += where_clause

        # execute queries to db
        with DB_ENG.connect() as con:
            meta = pd.read_sql(
                sql, con,
                index_col="station_id")

        # change to GeoDataFrame if geometry column was selected
        for geom_col, srid in zip(["geometry", "geometry_utm"],
                                  ["4326",     "25832"]):
            if geom_col in infos:
                meta[geom_col] = meta[geom_col].apply(wkt.loads)
                meta = gpd.GeoDataFrame(
                    meta, crs="EPSG:" + srid, geometry=geom_col)

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
        meta = self.get_meta(
            infos=["station_id"], only_real=only_real, stids=stids)

        if (type(stids) == str) and (stids == "all"):
            stations = [
                self._StationClass(stid, _skip_meta_check=True) 
                for stid in meta.index]
        else:
            stids = list(stids)
            stations = [
                self._StationClass(stid, _skip_meta_check=True) 
                for stid in meta.index 
                if stid in stids]
            stations_ids = [stat.id for stat in stations]
            if len(stations) != len(stids):
                raise ValueError(
                    "It was not possible to create a {para_long} Station with the following IDs: {stids}".format(
                        para_long=self._para_long,
                        stids = ", ".join([stid for stid in stids if stid in stations_ids])
                    ))

        return stations

    def count_holes(self, stids="all", **kwargs):
        """Count holes in timeseries depending on there length.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kwargs : dict, optional
            This is a list of parameters, that is supported by the StationBase.count_holes method.
            E.G.:
            weeks : list, optional
                A list of hole length to count. 
                Every hole longer than the duration of weeks specified is counted.
                The default is [2, 4, 8, 12, 16, 20, 24]
            kind : str
                The kind of the timeserie to analyze.
                Should be one of ['raw', 'qc', 'filled']. 
                For N also "corr" is possible.
                Normally only "raw" and "qc" make sense, because the other timeseries should not have holes.
            period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
                The minimum and maximum Timestamp for which to analyze the timeseries.
                If None is given, the maximum and minimal possible Timestamp is taken.
                The default is (None, None).
            between_meta_period : bool, optional
                Only check between the respective period that is defined in the meta table.
                If "qc" is chosen as kind, then the "raw" meta period is taken.
                The default is True.
            crop_period : bool, optional
                should the period get cropped to the maximum filled period.
                This will result in holes being ignored when they are at the end or at the beginning of the timeserie.
                If period = (None, None) is given, then this parameter is set to True.
                The default is False.

        Returns
        -------
        pandas.DataFrame
            A Pandas Dataframe, with station_id as index and one column per week.
            The numbers in the table are the amount of NA-periods longer than the respective amount of weeks.

        Raises
        ------
        ValueError
            If the input parameters were not correct.
        """   
        # check input parameters
        stations = self.get_stations(stids=stids, only_real=True)
            
        # iter stations
        first = True
        for station in pb.progressbar(stations, line_breaks=False):
            new_count = station.count_holes(**kwargs)
            if first:
                meta = new_count
                first = False
            else:
                meta = pd.concat([meta, new_count], axis=0)
        
        return meta       

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

    def _run_method(self, stations, method, name, kwds=dict(), 
            do_mp=True, processes=mp.cpu_count()-1):
        """Run methods of the given stations objects in multiprocessing/threading mode.

        Parameters
        ----------
        stations : list of station objects
            A list of station objects. Those must be children of the StationBase class.
        method : str
            The name of the method to call.
        name : str
            A descriptive name of the method to show in the progressbar.
        kwds : dict
            The keyword arguments to give to the methods
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is True.
        processes : int, optional
            The number of processes that should get started simultaneously.
            If 1 or less, then the process is computed as a simple loop, so there is no multiprocessing or threading done.
            The default is the cpu count -1.
        """
        log.info("-"*79 +
            f"\n{self._para_long} Stations async loop over method '{method}' started."
            )

        if processes<=1:
            log.info(f"Ass the number of processes is 1 or lower, the method '{method}' is started as a simple loop.")
            self._run_simple_loop(
                stations=stations, method=method, name=name, kwds=kwds)
        else:
            # progressbar
            num_stations = len(stations)
            pbar = self._get_progressbar(max_value=num_stations, name=name)

            # create pool
            if do_mp:
                try:
                    pool = mp.Pool(processes=processes)
                    log.debug("the multiprocessing Pool is started")
                except AssertionError:
                    log.debug('daemonic processes are not allowed to have children, therefor threads are used')
                    pool = ThreadPool(processes=processes)
            else:
                log.debug("the threading Pool is started")
                pool = ThreadPool(processes=processes)

            # start processes
            results = []
            for stat in stations:
                results.append(
                    pool.apply_async(
                        getattr(stat, method),
                        kwds=kwds))
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

            pbar.update(sum(finished))
            pool.join()
            pool.terminate()

    def _run_simple_loop(self, stations, method, name, kwds=dict()):
        log.info("-"*79 +
        "\n{para_long} Stations simple loop over method '{method}' started.".format(
            para_long=self._para_long,
            method=method
        ))

        # progressbar
        num_stations = len(stations)
        pbar = self._get_progressbar(max_value=num_stations, name=name)

        # start processes
        results = []
        for stat in stations:
            getattr(stat, method)(**kwds)
            pbar.variables["last_station"] = stat.id
            pbar.update(pbar.value + 1)

    @check_superuser
    def update_raw(self, only_new=True, only_real=True, stids="all", 
            remove_nas=True, do_mp=True, **kwargs):
        """Download all stations data from CDC and upload to database.

        Parameters
        ----------
        only_new : bool, optional
            Get only the files that are not yet in the database?
            If False all the available files are loaded again.
            The default is True
        only_real: bool, optional
            Whether only real stations are tried to download.
            True: only stations with a date in raw_from in meta are downloaded.
            The default is True.
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".  
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is True.  
        remove_nas : bool, optional
            Remove the NAs from the downloaded data before updating it to the database. 
            This has computational advantages.
            The default is True.    
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        start_tstp = datetime.datetime.now()

        # get FTP file list
        # CDC.login()

        ftp_file_list = get_cdc_file_list(
            # ftp_conn=get_cdc_con(), #CDC,
            ftp_folders=self._ftp_folders)

        # run the tasks in multiprocessing mode
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids),
            method="update_raw",
            name="download raw {para} data".format(para=self._para.upper()),
            kwds=dict(
                only_new=only_new,
                ftp_file_list=ftp_file_list,
                remove_nas=remove_nas),
            do_mp=do_mp, **kwargs)

        # save start time as variable to db
        if (type(stids) == str) and (stids == "all"):
            with DB_ENG.connect() as con:
                con.execute(sqltxt("""
                    UPDATE para_variables
                    SET start_tstp_last_imp='{start_tstp}'::timestamp,
                    max_tstp_last_imp=(SELECT max(raw_until) FROM meta_{para})
                    WHERE para='{para}';
                """.format(
                    para=self._para,
                    start_tstp=start_tstp.strftime("%Y%m%d %H:%M"))))

    @check_superuser
    def last_imp_quality_check(self, stids="all", do_mp=False, **kwargs):
        """Do the quality check of the last import.

        Parameters
        ----------
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is False.
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method
        """
        self._run_method(
            stations=self.get_stations(only_real=True, stids=stids),
            method="last_imp_quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)

    @check_superuser
    def last_imp_fillup(self, stids="all", do_mp=False, **kwargs):
        """Do the gap filling of the last import.

        Parameters
        ----------
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is False.
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method
        """
        stations = self.get_stations(only_real=False, stids=stids)
        period = stations[0].get_last_imp_period(all=True)
        period_log = period.strftime("%Y-%m-%d %H:%M")
        log.info("The {para_long} Stations fillup of the last import is started for the period {min_tstp} - {max_tstp}".format(
            para_long=self._para_long,
            min_tstp=period_log[0],
            max_tstp=period_log[1]))
        self._run_method(
            stations=stations,
            method="last_imp_fillup",
            name="fillup {para} data".format(para=self._para.upper()),
            kwds=dict(_last_imp_period=period),
            do_mp=do_mp, **kwargs)

    @check_superuser
    def quality_check(self, period=(None, None), only_real=True, stids="all",
            do_mp=False, **kwargs):
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
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is False.
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method
        """
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids),
            method="quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            kwds=dict(period=period),
            do_mp=do_mp, **kwargs)

    @check_superuser
    def update_ma(self, stids="all", do_mp=False, **kwargs):
        """Update the multi annual values for the stations.

        Get a multi annual value from the corresponding raster and save to the multi annual table in the database.

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
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=False, stids=stids),
            method="update_ma",
            name="update ma-values for {para}".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)

    @check_superuser
    def fillup(self, only_real=False, stids="all", do_mp=False, **kwargs):
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
        do_mp : bool, optional
            Should the method be done in multiprocessing mode?
            If False the methods will be called in threading mode.
            Multiprocessing needs more memory and a bit more initiating time. Therefor it is only usefull for methods with a lot of computation effort in the python code.
            If the most computation of a method is done in the postgresql database, then threading is enough to speed the process up.
            The default is False.
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids),
            method="fillup",
            name="fillup {para} data".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)
    
    @check_superuser
    def update(self, only_new=True, **kwargs):
        """Make a complete update of the stations.

        Does the update_raw, quality check and fillup of the stations.

        Parameters
        ----------
        only_new : bool, optional
            Should a only new values be computed?
            If False: The stations are updated for the whole possible period.
            If True, the stations are only updated for new values.
            The default is True.
        """        
        self.update_raw(only_new=only_new, **kwargs)
        if only_new:
            self.last_imp_quality_check(**kwargs)
            self.last_imp_fillup(**kwargs)
        else:
            self.quality_check(**kwargs)
            self.fillup(**kwargs)

    def get_df(self, stids, kind, **kwargs):
        """Get a DataFrame with the corresponding data.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kwargs: optional keyword arguments
            Those keyword arguments are passed to the get_df function of the station class.
            can be period, agg_to, kinds

        Returns
        -------
        pd.Dataframe
            A DataFrame with the timeseries for this station and the given period.
        """
        if "kinds" in kwargs:
            raise ValueError("The kinds parameter is not supported for stations objects. Please use kind instead.")
        stats = self.get_stations(only_real=False, stids=stids)
        for stat in pb.progressbar(stats, line_breaks=False):
            df = stat.get_df(kinds=[kind], **kwargs)
            df.rename(
                dict(zip(
                    df.columns, 
                    [str(stat.id) for col in df.columns])), 
                axis=1, inplace=True)
            if "df_all" in locals():
                df_all = df_all.join(df)
            else:
                df_all = df
        
        return df_all


class StationsTETBase(StationsBase):
    @check_superuser
    def fillup(self, only_real=False, stids="all"):
        # create virtual stations if necessary
        if not only_real:
            meta = self.get_meta(
                infos=["Station_id"], only_real=False)
            meta_n = StationsN().get_meta(
                infos=["Station_id"], only_real=False)
            stids_missing = set(meta_n.index.values) - set(meta.index.values)
            if stids != "all":
                stids_missing = set(stids).intersection(stids_missing)
            for stid in stids_missing:
                self._StationClass(stid) # this creates the virtual station

        super().fillup(only_real=only_real,stids=stids)

# Implement stations classes
class StationsN(StationsBase):
    """A class to work with and download 10 minutes precipitation data for several stations."""
    _StationClass = StationN
    _timeout_raw_imp = 360

    @check_superuser
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
        kwargs : dict, optional
            The keyword arguments to be handed to the station.StationN.update_richter_class method.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=True, stids=stids),
            method="update_richter_class",
            name="update richter class for {para}".format(para=self._para.upper()),
            kwds=kwargs,
            do_mp=do_mp)

    @check_superuser
    def richter_correct(self, stids="all", **kwargs):
        """Richter correct the filled data.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=False, stids=stids),
            method="richter_correct",
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=False, **kwargs)

    @check_superuser
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
        kwargs : dict, optional
            The additional keyword arguments for the _run_method method

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
        self._run_method(
            stations=stations,
            method="last_imp_corr",
            kwds={"_last_imp_period": period},
            name="richter correction on {para}".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)

    @check_superuser
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

class StationsND(StationsBase):
    """A class to work with and download daily precipitation data for several stations.
    
    Those stations data are only downloaded to do some quality checks on the 10 minutes data.
    Therefor there is no special quality check and richter correction done on this data.
    If you want daily precipitation data, better use the 10 minutes station class (StationN) and aggregate to daily values.
    """
    _StationClass = StationND
    _StationClass_parent = StationsN
    _timeout_raw_imp = 120


class StationsT(StationsTETBase):
    """A class to work with and download temperature data for several stations."""
    _StationClass = StationT
    _timeout_raw_imp = 120


class StationsET(StationsTETBase):
    """A class to work with and download potential Evapotranspiration (VPGB) data for several stations."""
    _StationClass = StationET
    _timeout_raw_imp = 120


# create a grouping class
class GroupStations(object):
    """A class to group all possible parameters of all the stations.
    """
    _StationN = StationN
    _GroupStation = GroupStation

    def __init__(self):
        self.stationsN = StationsN()

    def get_valid_stids(self):
        if not hasattr(self, "_valid_stids"):
            sql ="""
            SELECT station_id FROM meta_n"""
            with DB_ENG.connect() as con:
                res = con.execute(sqltxt(sql))
            self._valid_stids = [el[0] for el in res.all()]
        return self._valid_stids

    def _check_paras(self, paras):
        if type(paras)==str and paras != "all":
            paras = [paras,]
        
        valid_paras=["n", "t", "et"]
        if (type(paras) == str) and (paras == "all"):
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
        for stid in stids:
            max_period_i = self._GroupStation(stid).get_max_period(
                kinds=kinds, nas_allowed=nas_allowed)
            if "max_period" in locals():
                max_period = max_period.union(
                    max_period_i, 
                    how="outer" if nas_allowed else "inner"
                )
            else:
                max_period=max_period_i
        
        if type(period) != TimestampPeriod:
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
        if (type(stids) == str) and (stids == "all"):
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
            meta_para["para"] = stat._para
            if "meta_all" not in locals():
                meta_all = meta_para
            else:
                meta_all = pd.concat([meta_all, meta_para], axis=0)
                if type(meta_para)==gpd.GeoDataFrame:
                    meta_all = gpd.GeoDataFrame(meta_all, crs=meta_para.crs)
        
        if len(paras)==1:
            return meta_all.drop("para", axis=1)
        else:
            return meta_all.reset_index().set_index(["station_id", "para"]).sort_index()

    def get_para_stations(self, paras="all"):
        """Get a list with all the multi parameter stations as stations.Station\{parameter\}-objects.

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

        if (type(stids) == str) and (stids == "all"):
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
                  add_t_min=False, add_t_max=False, **kwargs):
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
            con.execute(sqltxt(sql_save_time))

        # create log message
        log.debug(
            "The timeseries tables for {quantity} stations got created in {dir}".format(
                quantity=len(stids), dir=dir))

    def create_roger_ts(self, dir, period=(None, None), stids="all",
                        kind="best", r_r0=1, 
                        add_t_min=False, add_t_max=False, **kwargs):
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
        **kwargs: 
            additional parameters for GroupStation.create_ts

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        return self.create_ts(dir=dir, period=period, kinds=kind,
                              agg_to="10 min", r_r0=r_r0, stids=stids,
                              split_date=True, nas_allowed=False,
                              add_t_min=add_t_min, add_t_max=add_t_max,
                              **kwargs)

# clean station
del StationN, StationND, StationT, StationET, GroupStation, StationBase