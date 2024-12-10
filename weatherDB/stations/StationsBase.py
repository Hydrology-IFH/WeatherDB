# libraries
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
from sqlalchemy import text as sqltxt
import sqlalchemy as sa
import textwrap

from ..db.connections import db_engine
from ..utils.dwd import get_dwd_meta, get_cdc_file_list
from ..station.StationBases import StationBase
from ..db import models
from ..db.queries.get_quotient import _get_quotient

# set settings
# ############
try:# else I get strange errors with linux
    mp.set_start_method('spawn')
except RuntimeError:
    pass

__all__ = ["StationsBase"]
log = logging.getLogger(__name__)

# Base class definitions
########################

class StationsBase:
    _StationClass = StationBase
    _timeout_raw_imp = 240

    def __init__(self):
        if type(self) is StationsBase:
            raise NotImplementedError("""
            The StationsBase is only a wrapper class an is not working on its own.
            Please use StationP, StationPD, StationT or StationET instead""")
        self._ftp_folder_base = self._StationClass._ftp_folder_base
        if isinstance(self._ftp_folder_base, str):
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
            if isinstance(meta_new, gpd.GeoDataFrame):
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

    @db_engine.deco_update_privilege
    def update_meta(self, stids="all", **kwargs):
        """Update the meta table by comparing to the CDC server.

        The "von_datum" and "bis_datum" is ignored because it is better to set this by the filled period of the stations in the database.
        Often the CDC period is not correct.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        """
        log.info(
            "The {para_long} meta table gets updated."\
                .format(para_long=self._para_long))
        meta = self.download_meta()

        # check if Abgabe is in meta
        if "Abgabe" in meta.columns:
            meta.drop("Abgabe", axis=1, inplace=True)

        # get dropped stations and delete from meta file
        sql_get_dropped = sa\
            .select(models.DroppedStations.station_id)\
            .where(models.DroppedStations.parameter == self._para)
        with db_engine.connect() as con:
            dropped_stids = con.execute(sql_get_dropped).all()
        dropped_stids = [row[0] for row in dropped_stids
                        if row[0] in meta.index]
        meta.drop(dropped_stids, inplace=True)

        # check if only some stids should be updated
        if stids != "all":
            if not isinstance(stids, list):
                stids = [stids,]
            meta.drop([stid for stid in meta.index if stid not in stids], inplace=True)

        # to have a meta entry for every station before looping over them
        if "von_datum" in meta.columns and "bis_datum" in meta.columns:
            self._update_db_meta(
                meta=meta.drop(["von_datum", "bis_datum"], axis=1))
        else:
            self._update_db_meta(meta=meta)

        log.info(
            "The {para_long} meta table got successfully updated."\
                .format(para_long=self._para_long))

    @db_engine.deco_update_privilege
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
        with db_engine.connect() as con:
            columns_db = con.execute(sqltxt(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name='meta_{para}';
                """.format(para=self._para)
                )).all()
            columns_db = [col[0] for col in columns_db]

        problem_cols = [col for col in columns if col not in columns_db]
        if len(problem_cols) > 0:
            warnings.warn("""
                The meta_{para} column '{cols}' is not initiated in the database.
                This column is therefor skiped.
                Please review the DB or the code.
                """.format(
                    para=self._para,
                    cols=", ".join(problem_cols))
                    )
            columns = [col for col in columns if col in columns_db]

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
        with db_engine.connect() as con:
            con.execute(sqltxt(sql))
            con.commit()

    @db_engine.deco_update_privilege
    def update_period_meta(self, stids="all", **kwargs):
        """Update the period in the meta table of the raw data.

        Parameters
        ----------
        stids: string  or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs : dict, optional
        **kwargs : dict, optional
            The additional keyword arguments are passed to the get_stations method.

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_simple_loop(
            stations=self.get_stations(only_real=True, stids=stids, **kwargs),
            method="update_period_meta",
            name="update period in meta",
            kwargs=kwargs
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
        if isinstance(infos, str):
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
            warnings.warn(textwrap.dedent("""\
                You selected 2 geometry columns.
                Only the geometry column with EPSG 4326 is returned"""))
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
            if not isinstance(stids, list):
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
        with db_engine.connect() as con:
            meta = pd.read_sql(
                sqltxt(sql),
                con,
                index_col="station_id")

        # make datetime columns timezone aware
        meta = meta.apply(
            lambda col: col.dt.tz_localize(datetime.timezone.utc) \
                if hasattr(col, "dt") and not col.dt.tz else col)

        # change to GeoDataFrame if geometry column was selected
        for geom_col, srid in zip(["geometry", "geometry_utm"],
                                  ["4326",     "25832"]):
            if geom_col in infos:
                meta[geom_col] = meta[geom_col].apply(wkt.loads)
                meta = gpd.GeoDataFrame(
                    meta, crs="EPSG:" + srid, geometry=geom_col)

        # strip whitespaces in string columns
        for col in meta.columns[meta.dtypes == "object"]:
            try:
                meta[col] = meta[col].str.strip()
            except:
                pass

        return meta

    def get_stations(self, only_real=True, stids="all", skip_missing_stids=False, **kwargs):
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
        skip_missing_stids: bool, optional
            Should the method skip the missing stations from input stids?
            If False, then a ValueError is raised if a station is not found.
            The default is False.
        **kwargs : dict, optional
            The additional keyword arguments aren't used in this method.

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

        if isinstance(stids, str) and (stids == "all"):
            stations = [
                self._StationClass(stid, _skip_meta_check=True)
                for stid in meta.index]
        else:
            stids = list(stids)
            stations = [
                self._StationClass(stid, _skip_meta_check=True)
                for stid in meta.index
                if stid in stids]
            if (not skip_missing_stids) and (len(stations) != len(stids)):
                stations_ids = [stat.id for stat in stations]
                raise ValueError(
                    "It was not possible to create a {para_long} Station with the following IDs: {stids}".format(
                        para_long=self._para_long,
                        stids = ", ".join([str(stid) for stid in stids if stid not in stations_ids])
                    ))

        return stations

    def get_quotient(self, kinds_num, kinds_denom, stids="all", return_as="df",  **kwargs):
        """Get the quotient of multi-annual means of two different kinds or the timeserie and the multi annual raster value.

        $quotient = \overline{ts}_{kind_num} / \overline{ts}_{denom}$

        Parameters
        ----------
        kinds_num : list of str or str
            The timeseries kinds of the numerators.
            Should be one of ['raw', 'qc', 'filled'].
            For precipitation also "corr" is possible.
        kinds_denom : list of str or str
            The timeseries kinds of the denominator or the multi annual raster key.
            If the denominator is a multi annual raster key, then the result is the quotient of the timeserie and the raster value.
            Possible values are:
                - for timeserie kinds: 'raw', 'qc', 'filled' or for precipitation also "corr".
                - for raster keys: 'hyras', 'dwd' or 'regnie', depending on your defined raster files.
        stids : list of Integer
            The stations IDs for which to compute the quotient.
        return_as : str, optional
            The format of the return value.
            If "df" then a pandas DataFrame is returned.
            If "json" then a list with dictionaries is returned.
        **kwargs : dict, optional
            The additional keyword arguments are passed to the get_stations method.

        Returns
        -------
        pandas.DataFrame or list of dict
            The quotient of the two timeseries as DataFrame or list of dictionaries (JSON) depending on the return_as parameter.
            The default is pd.DataFrame.

        Raises
        ------
        ValueError
            If the input parameters were not correct.
        """
        # check stids
        if stids == "all":
            stids = None

        # check kinds
        rast_keys = {"hyras", "regnie", "dwd"}
        kinds_num = self._StationClass._check_kinds(kinds_num)
        kinds_denom = self._StationClass._check_kinds(
            kinds_denom,
            valids=self._StationClass._valid_kinds | rast_keys)

        # get quotient
        with db_engine.connect() as con:
            return _get_quotient(
                con=con,
                stids=stids,
                paras=self._para,
                kinds_num=kinds_num,
                kinds_denom=kinds_denom,
                return_as=return_as)

    def count_holes(self, stids="all", **kwargs):
        """Count holes in timeseries depending on there length.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations to return.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs : dict, optional
        **kwargs : dict, optional
            This is a list of parameters, that is supported by the StationBase.count_holes method.

            Furthermore the kwargs are passed to the get_stations method.

            possible values are:

            - weeks : list, optional
                A list of hole length to count.
                Every hole longer than the duration of weeks specified is counted.
                The default is [2, 4, 8, 12, 16, 20, 24]
            - kind : str
                The kind of the timeserie to analyze.
                Should be one of ['raw', 'qc', 'filled'].
                For N also "corr" is possible.
                Normally only "raw" and "qc" make sense, because the other timeseries should not have holes.
            - period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
                The minimum and maximum Timestamp for which to analyze the timeseries.
                If None is given, the maximum and minimal possible Timestamp is taken.
                The default is (None, None).
            - between_meta_period : bool, optional
                Only check between the respective period that is defined in the meta table.
                If "qc" is chosen as kind, then the "raw" meta period is taken.
                The default is True.
            - crop_period : bool, optional
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
        stations = self.get_stations(stids=stids, only_real=True, **kwargs)

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
            do_mp=True, processes=mp.cpu_count()-1, **kwargs):
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
        log.info(
            f"{self._para_long} Stations async loop over method '{method}' started." +
            "\n" +"-"*80
            )

        if processes<=1:
            log.info(f"As the number of processes is 1 or lower, the method '{method}' is started as a simple loop.")
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
                if all(finished):
                    break

                for result in [result for i, result in enumerate(results)
                                    if not finished[i] and result.ready()]:
                    index = results.index(result)
                    finished[index] = True
                    pbar.variables["last_station"] = stations[index].id
                    # get stdout and log
                    header = f"""The {name} of the {self._para_long} Station with ID {stations[index].id} finished with """
                    try:
                        stdout = result.get(10)
                        if stdout is not None:
                            log.debug(f"{header}stdout:\n{result.get(10)}")
                    except Exception:
                        log.error(f"{header}stderr:\n{traceback.format_exc()}")

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
        for stat in stations:
            getattr(stat, method)(**kwds)
            pbar.variables["last_station"] = stat.id
            pbar.update(pbar.value + 1)

    @db_engine.deco_update_privilege
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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        start_tstp = datetime.datetime.now()

        # get FTP file list
        ftp_file_list = get_cdc_file_list(
            ftp_folders=self._ftp_folders)

        # run the tasks in multiprocessing mode
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids, **kwargs),
            method="update_raw",
            name="download raw {para} data".format(para=self._para.upper()),
            kwds=dict(
                only_new=only_new,
                ftp_file_list=ftp_file_list,
                remove_nas=remove_nas),
            do_mp=do_mp, **kwargs)

        # save start time as variable to db
        do_update_period = isinstance(stids, str) and (stids == "all")
        if not do_update_period and isinstance(stids, list):
            all_stids = self.get_meta(["station_id"], stids="all", only_real=True).index
            do_update_period = all([stid in stids for stid in all_stids])

        if do_update_period:
            with db_engine.connect() as con:
                con.execute(sqltxt("""
                    INSERT INTO parameter_variables (parameter, start_tstp_last_imp, max_tstp_last_imp)
                    VALUES ('{para}',
                            '{start_tstp}'::timestamp,
                            (SELECT max(raw_until) FROM meta_{para}))
                    ON CONFLICT (parameter) DO UPDATE SET
                        start_tstp_last_imp=EXCLUDED.start_tstp_last_imp,
                        max_tstp_last_imp=EXCLUDED.max_tstp_last_imp;
                """.format(
                    para=self._para,
                    start_tstp=start_tstp.strftime("%Y%m%d %H:%M"))))
                con.commit()

    @db_engine.deco_update_privilege
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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method
        """
        self._run_method(
            stations=self.get_stations(only_real=True, stids=stids, **kwargs),
            method="last_imp_quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            do_mp=do_mp, **kwargs)

    @db_engine.deco_update_privilege
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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method
        """
        stations = self.get_stations(only_real=False, stids=stids, **kwargs)
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
            do_mp=do_mp,
            **kwargs)

    @db_engine.deco_update_privilege
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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method
        """
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids, **kwargs),
            method="quality_check",
            name="quality check {para} data".format(para=self._para.upper()),
            kwds=dict(period=period),
            do_mp=do_mp,
            **kwargs)

    @db_engine.deco_update_privilege
    def update_ma_raster(self, stids="all", do_mp=False, **kwargs):
        """Update the multi annual raster values for the stations.

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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=False, stids=stids, **kwargs),
            method="update_ma_raster",
            name="update ma-raster-values for {para}".format(para=self._para.upper()),
            do_mp=do_mp,
            **kwargs)

    @db_engine.deco_update_privilege
    def update_ma_timeseries(self, kind, stids="all", do_mp=False, **kwargs):
        """Update the multi annual timeseries values for the stations.

        Get a multi annual value from the corresponding timeseries and save to the database.

        Parameters
        ----------
        kind : str or list of str
            The timeseries data kind to update theire multi annual value.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled".
            For the precipitation also "corr" is valid.
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
        self._run_method(
            stations=self.get_stations(only_real=False, stids=stids, **kwargs),
            method="update_ma_timeseries",
            name="update ma-ts-values for {para}".format(para=self._para.upper()),
            do_mp=do_mp,
            kwds=dict(kind=kind),
            **kwargs)

    @db_engine.deco_update_privilege
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
        **kwargs : dict, optional
            The additional keyword arguments for the _run_method and get_stations method

        Raises
        ------
        ValueError
            If the given stids (Station_IDs) are not all valid.
        """
        self._run_method(
            stations=self.get_stations(only_real=only_real, stids=stids, **kwargs),
            method="fillup",
            name="fillup {para} data".format(para=self._para.upper()),
            do_mp=do_mp,
            **kwargs)

    @db_engine.deco_update_privilege
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

    def get_df(self, stids, **kwargs):
        """Get a DataFrame with the corresponding data.

        Parameters
        ----------
        stids: string or list of int, optional
            The Stations for which to compute.
            Can either be "all", for all possible stations
            or a list with the Station IDs.
            The default is "all".
        **kwargs: optional keyword arguments
            Those keyword arguments are passed to the get_df function of the station class.
            Possible parameters are period, agg_to, kinds.
            Furthermore the kwargs are passed to the get_stations method.

        Returns
        -------
        pd.Dataframe
            A DataFrame with the timeseries for the selected stations, kind(s) and the given period.
            If multiple columns are selected, the columns in this DataFrame is a MultiIndex with the station IDs as first level and the kind as second level.
        """
        if "kinds" in kwargs and "kind" in kwargs:
            raise ValueError("Either enter kind or kinds, not both.")
        if "kind" in kwargs:
            kinds=[kwargs.pop("kind")]
        else:
            kinds=kwargs.pop("kinds")
        kwargs.update(dict(only_real=kwargs.get("only_real", False)))
        stats = self.get_stations(stids=stids, **kwargs)
        df_all = None
        for stat in pb.progressbar(stats, line_breaks=False):
            df = stat.get_df(kinds=kinds, **kwargs)
            if df is None:
                warnings.warn(
                    f"There was no data for {stat._para_long} station {stat.id}!")
                continue
            if len(df.columns) == 1:
                df.rename(
                    dict(zip(df.columns, [stat.id])),
                    axis=1, inplace=True)
            else:
                df.columns = pd.MultiIndex.from_product(
                    [[stat.id], df.columns],
                    names=["Station ID", "kind"])
            df_all = pd.concat([df_all, df], axis=1)

        return df_all
