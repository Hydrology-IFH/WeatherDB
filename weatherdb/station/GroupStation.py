# libraries
import logging
from datetime import datetime
from pathlib import Path
import warnings
import zipfile
from packaging import version
import pandas as pd

from ..utils.TimestampPeriod import TimestampPeriod
from .StationBases import StationBase, AGG_TO
from . import StationP, StationT, StationET

# set settings
# ############
__all__ = ["GroupStation"]
log = logging.getLogger(__name__)

# class definition
##################
class GroupStation(object):
    """A class to group all possible parameters of one station.

    So if you want to create the input files for a simulation, where you need T, ET and N, use this class to download the data for one station.
    """

    def __init__(self, id, error_if_missing=True, **kwargs):
        self.id = id
        self.station_parts = []
        self._error_if_missing = error_if_missing
        for StatClass in [StationP, StationT, StationET]:
            try:
                self.station_parts.append(
                    StatClass(id=id, **kwargs)
                )
            except Exception as e:
                if error_if_missing:
                    raise e
        self.paras_available = [stat._para for stat in self.station_parts]

    def _check_paras(self, paras):
        if isinstance(paras, str) and paras != "all":
            paras = [paras,]

        if isinstance(paras, str) and (paras == "all"):
            return self.paras_available
        else:
            paras_new = []
            for para in paras:
                if para in self.paras_available:
                    paras_new.append(para)
                elif self._error_if_missing:
                    raise ValueError(
                        f"The parameter {para} you asked for is not available for station {self.id}")
            return paras_new

    @staticmethod
    def _check_kinds(kinds):
        # type cast kinds
        if isinstance(kinds, str):
            kinds = [kinds]
        else:
            kinds = kinds.copy()
        return kinds

    def get_available_paras(self, short=False):
        """Get the possible parameters for this station.

        Parameters
        ----------
        short : bool, optional
            Should the short name of the parameters be returned.
            The default is "long".

        Returns
        -------
        list of str
            A list of the long parameter names that are possible for this station to get.
        """
        paras = []
        attr_name = "_para" if short else "_para_long"
        for stat in self.station_parts:
            paras.append(getattr(stat, attr_name))

        return paras

    def get_filled_period(self, kinds="best", from_meta=True, join_how="inner"):
        """Get the combined filled period for all 3 stations.

        This is the maximum possible timerange for these stations.

        Parameters
        ----------
        kind :  str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        from_meta : bool, optional
            Should the period be from the meta table?
            If False: the period is returned from the timeserie. In this case this function is only a wrapper for .get_period_meta.
            The default is True.
        join_how : str, optional
            How should the different periods get joined.
            If "inner" then the minimal period that is inside of all the filled_periods is returned.
            If "outer" then the maximal possible period is returned.
            The default is "inner".

        Returns
        -------
        TimestampPeriod
            The maximum filled period for the 3 parameters for this station.
        """
        kinds = self._check_kinds(kinds)
        for kind in ["filled_by", "adj"]:
            if kind in kinds:
                kinds.remove(kind)

        # get filled_period
        for kind in kinds:
            for stat in self.station_parts:
                new_filled_period =  stat.get_filled_period(
                    kind=kind, from_meta=from_meta)

                if "filled_period" not in locals():
                    filled_period = new_filled_period.copy()
                else:
                    filled_period = filled_period.union(
                        new_filled_period, how=join_how)

        return filled_period

    def get_df(self, period=(None, None), kinds="best", paras="all",
               agg_to="day", nas_allowed=True, add_na_share=False,
               add_t_min=False, add_t_max=False, **kwargs):
        """Get a DataFrame with the corresponding data.

        Parameters
        ----------
        period : TimestampPeriod or (tuple or list of datetime.datetime or None), optional
            The minimum and maximum Timestamp for which to get the timeseries.
            If None is given, the maximum or minimal possible Timestamp is taken.
            The default is (None, None).
        kinds :  str or list of str
            The data kind to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by", "best"("corr" for N and "filled" for T and ET).
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For Precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
        agg_to : str, optional
            To what aggregation level should the timeseries get aggregated to.
            The minimum aggregation for Temperatur and ET is daily and for the precipitation it is 10 minutes.
            If a smaller aggregation is selected the minimum possible aggregation for the respective parameter is returned.
            So if 10 minutes is selected, than precipitation is returned in 10 minuets and T and ET as daily.
            The default is "10 min".
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is True.
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
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

        Returns
        -------
        pd.Dataframe
            A DataFrame with the timeseries for this station and the given period.
        """
        paras = self._check_paras(paras)

        # download dataframes
        dfs = []
        for stat in self.station_parts:
            if stat._para in paras:
                # check if min and max for temperature should get added
                use_kinds = kinds.copy()
                if stat._para == "t":
                    if isinstance(use_kinds, str):
                        use_kinds=[use_kinds]
                    if "best" in use_kinds:
                        use_kinds.insert(use_kinds.index("best"), "filled")
                        use_kinds.remove("best")
                    for k in ["raw", "filled"]:
                        if k in use_kinds:
                            if add_t_max:
                                use_kinds.insert(
                                    use_kinds.index(k)+1,
                                    f"{k}_max")
                            if add_t_min:
                                use_kinds.insert(
                                    use_kinds.index(k)+1,
                                    f"{k}_min")

                # get the data from station object
                df = stat.get_df(
                    period=period,
                    kinds=use_kinds,
                    agg_to=agg_to,
                    nas_allowed=nas_allowed,
                    add_na_share=add_na_share,
                    **kwargs)
                df = df.rename(dict(zip(
                    df.columns,
                    [stat._para.upper() + "_" + col for col in df.columns])),
                    axis=1)
                dfs.append(df)

        # concat the dfs
        if len(dfs) > 1:
            df_all = pd.concat(dfs, axis=1)
        elif len(dfs) == 1 :
            df_all = dfs[0]
        else:
            raise ValueError("No timeserie was found for {paras} and Station {stid}".format(
                paras=", ".join(paras),
                stid=self.id))

        return df_all

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
        return StationBase.get_meta_explanation(infos=infos)

    def get_max_period(self, kinds, nas_allowed=False):
        """Get the maximum available period for this stations timeseries.

        If nas_allowed is True, then the maximum range of the timeserie is returned.
        Else the minimal filled period is returned

        Parameters
        ----------
        kinds : str or list of str
            The data kinds to update.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj".
            For the precipitation also "qn" and "corr" are valid.
        nas_allowed : bool, optional
            Should NAs be allowed?
            If True, then the maximum possible period is returned, even if there are NAs in the timeserie.
            If False, then the minimal filled period is returned.
            The default is False.

        Returns
        -------
        utils.TimestampPeriod
            The maximum Timestamp Period
        """
        kinds = self._check_kinds(kinds)
        max_period = None
        for stat in self.station_parts:
            max_period_i = stat.get_max_period(
                kinds=kinds, nas_allowed=nas_allowed)
            if max_period is None:
                max_period = max_period_i
            else:
                max_period = max_period.union(
                    max_period_i,
                    how="outer" if nas_allowed else "inner")

        return max_period

    def get_meta(self, paras="all", **kwargs):
        """Get the meta information for every parameter of this station.

        Parameters
        ----------
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
        **kwargs : dict, optional
            The optional keyword arguments are handed to the single Station get_meta methods. Can be e.g. "info".

        Returns
        -------
        dict
            dict with the information.
            there is one subdict per parameter.
            If only one parameter is asked for, then there is no subdict, but only a single value.
        """
        paras = self._check_paras(paras)

        for stat in self.station_parts:
            if stat._para in paras:
                meta_para = stat.get_meta(**kwargs)
                if "meta_all" not in locals():
                    meta_all = {stat._para:meta_para}
                else:
                    meta_all.update({stat._para:meta_para})
        return meta_all

    def get_geom(self, crs=None):
        """Get the point geometry of the station.

        Parameters
        ----------
        crs: str, int or None, optional
            The coordinate reference system of the geometry.
            If None, then the geometry is returned in WGS84 (EPSG:4326).
            If string, then it should be in a pyproj readable format.
            If int, then it should be the EPSG code.
            The default is None.

        Returns
        -------
        shapely.geometries.Point
            The location of the station as shapely Point in the given coordinate reference system.
        """
        return self.station_parts[0].get_geom(crs=crs)

    def get_name(self):
        return self.station_parts[0].get_name()

    def create_roger_ts(self, dir, period=(None, None),
                        kind="best", r_r0=1, add_t_min=False, add_t_max=False,
                        do_toolbox_format=False,
                        **kwargs):
        """Create the timeserie files for roger as csv.

        This is only a wrapper function for create_ts with some standard settings.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
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
        r_r0 : int or float, list of int or float or None, optional
            Should the ET timeserie contain a column with R/R0.
            If None, then no column is added.
            If int or float, then a R/R0 column is appended with this number as standard value.
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appanded to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The serie is then joined to the ET timeserie.
            The default is 1.
        add_t_min=False : bool, optional
            Schould the minimal temperature value get added?
            The default is False.
        add_t_max=False : bool, optional
            Schould the maximal temperature value get added?
            The default is False.
        do_toolbox_format : bool, optional
            Should the timeseries be saved in the RoGeR toolbox format? (have a look at the RoGeR examples in https://github.com/Hydrology-IFH/roger)
            The default is False.
        **kwargs:
            additional parameters for Station.get_df

        Raises
        ------
        Warning
            If there are NAs in the timeseries or the period got changed.
        """
        if do_toolbox_format:
            return self.create_ts(
                dir=dir, period=period, kinds=kind,
                agg_to="10 min", r_r0=r_r0, split_date=True,
                nas_allowed=False,
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
                agg_to="10 min", r_r0=r_r0, split_date=True,
                nas_allowed=False,
                add_t_min=add_t_min, add_t_max=add_t_max,
                **kwargs)

    def create_ts(self, dir, period=(None, None),
                  kinds="best", paras="all",
                  agg_to="10 min", r_r0=None, split_date=False,
                  nas_allowed=True, add_na_share=False,
                  add_t_min=False, add_t_max=False,
                  add_meta=True, file_names={}, col_names={},
                  keep_date_parts=False,
                  **kwargs):
        """Create the timeserie files as csv.

        Parameters
        ----------
        dir : pathlib like object or zipfile.ZipFile
            The directory or Zipfile to store the timeseries in.
            If a zipfile is given a folder with the statiopns ID is added to the filepath.
        period : TimestampPeriod like object, optional
            The period for which to get the timeseries.
            If (None, None) is entered, then the maximal possible period is computed.
            The default is (None, None)
        kinds :  str or list of str
            The data kinds to look for filled period.
            Must be a column in the timeseries DB.
            Must be one of "raw", "qc", "filled", "adj", "filled_by", "filled_share", "best".
            If "best" is given, then depending on the parameter of the station the best kind is selected.
            For precipitation this is "corr" and for the other this is "filled".
            For the precipitation also "qn" and "corr" are valid.
            If only one kind is asked for, then the columns get renamed to only have the parameter name as column name.
        paras : list of str or str, optional
            Give the parameters for which to get the meta information.
            Can be "n", "t", "et" or "all".
            If "all", then every available station parameter is returned.
            The default is "all"
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
            If list of int or floats, then the list should have the same length as the ET-timeserie and is appanded to the Timeserie.
            If pd.Series, then the index should be a timestamp index. The serie is then joined to the ET timeserie.
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
        add_t_min=False : bool, optional
            Should the minimal temperature value get added?
            The default is False.
        add_t_max=False : bool, optional
            Should the maximal temperature value get added?
            The default is False.
        add_meta : bool, optional
            Should station Meta information like name and Location (lat, long) be added to the file?
            The default is True.
        file_names : dict, optional
            A dictionary with the file names for the different parameters.
            e.g.{"N":"PREC.txt", "T":"TA.txt", "ET":"ET.txt"}
            If an empty dictionary is given, then the standard names are used.
            The default is {}.
        col_names : dict, optional
            A dictionary with the column names for the different parameters.
            e.g.{"N":"PREC", "T":"TA", "ET":"ET", "Jahr":"YYYY", "Monat":"MM", "Tag":"DD", "Stunde":"HH", "Minute":"MN"}
            If an empty dictionary is given, then the standard names are used.
            The default is {}.
        keep_date_parts : bool, optional
            only used if split_date is True.
            Should the date parts that are not needed, e.g. hour value for daily timeseries, be kept?
            If False, then the columns that are not needed are dropped.
            The default is False.
        **kwargs:
            additional parameters for Station.get_df

        Raises
        ------
        Warning
            If there are NAs in the timeseries and nas_allowed is False
            or the period got changed.
        """
        # check directory
        dir = self._check_dir(dir)

        # type cast kinds
        kinds = self._check_kinds(kinds)
        paras = self._check_paras(paras)

        # get the period
        if not ("_skip_period_check" in kwargs and kwargs["_skip_period_check"]):
            period = TimestampPeriod._check_period(period).expand_to_timestamp()
            period_filled = self.get_filled_period(
                kinds=kinds,
                join_how="outer" if nas_allowed else "inner")

            if period.is_empty():
                period = period_filled
            else:
                period_new = period_filled.union(
                    period,
                    how="inner")
                if period_new != period:
                    warnings.warn(
                        f"The Period for Station {self.id} got changed from {str(period)} to {str(period_new)}.")
                    period = period_new
        if "_skip_period_check" in kwargs:
            del kwargs["_skip_period_check"]

        # prepare loop
        name_suffix = "_{stid:0>5}.txt".format(stid=self.id)
        x, y = self.get_geom().coords.xy
        name = self.get_name() + " (ID: {stid})".format(stid=self.id)
        do_zip = isinstance(dir, zipfile.ZipFile)

        for para in paras:
            # get the timeserie
            df = self.get_df(
                period=period, kinds=kinds,
                paras=[para], agg_to=agg_to,
                nas_allowed=nas_allowed,
                add_na_share=add_na_share,
                add_t_min=add_t_min, add_t_max=add_t_max,
                _skip_period_check=True,
                **kwargs)

            # rename columns
            if len(kinds)==1 or ("filled_by" in kinds and len(kinds)==2):
                if len(kinds)==1:
                    colname_base = [col for col in df.columns if len(col.split("_"))==2][0]
                else:
                    colname_base = f"{para.upper()}_" + kinds[1-(kinds.index("filled_by"))]
                df.rename(
                    {colname_base: para.upper(),
                     f"{colname_base}_min": f"{para.upper()}_min",
                     f"{colname_base}_max": f"{para.upper()}_max",},
                    axis=1, inplace=True)
            else:
                df.rename(
                    dict(zip(df.columns,
                            [col.replace(f"{para}_", f"{para.upper()}_")
                            for col in df.columns])),
                    axis=1, inplace=True)

            # check for NAs
            filled_cols = [col for col in df.columns if "filled_by" in col]
            if not nas_allowed and df.drop(filled_cols, axis=1).isna().sum().sum() > 0:
                warnings.warn("There were NAs in the timeserie for Station {stid}.".format(
                    stid=self.id))

            # special operations for et
            if para == "et" and r_r0 is not None:
                if isinstance(r_r0, int) or isinstance(r_r0, float):
                    df = df.join(
                        pd.Series([r_r0]*len(df), name="R/R0", index=df.index))
                elif isinstance(r_r0, pd.Series):
                    df = df.join(r_r0.rename("R_R0"))
                elif isinstance(r_r0, list):
                    df = df.join(
                        pd.Series(r_r0, name="R/R0", index=df.index))

            # create tables
            if split_date:
                n_parts = 5 if keep_date_parts else AGG_TO[agg_to]["split"][para]
                df = self._split_date(df.index)\
                        .iloc[:, 0:n_parts]\
                        .join(df)
            else:
                df.reset_index(inplace=True)

            # rename columns if user asked for
            df.rename(col_names, axis=1, inplace=True)

            # create header
            if add_meta:
                header = ("Name: " + name + "\t" * (len(df.columns)-1) + "\n" +
                        "Lat: " + y + "   ,Lon: " + x + "\t" * (len(df.columns)-1) + "\n")
            else:
                header = ""

            # get file name
            if para.upper() in file_names:
                file_name = file_names[para.upper()]
            elif para in file_names:
                file_name = file_names[para]
            else:
                file_name = para.upper() + name_suffix

            # write table out
            if version.parse(pd.__version__) > version.parse("1.5.0"):
                to_csv_kwargs = dict(lineterminator="\n")
            else:
                to_csv_kwargs = dict(line_terminator="\n")
            str_df = header + df.to_csv(
                sep="\t", decimal=".", index=False, **to_csv_kwargs)

            if do_zip:
                dir.writestr(f"{self.id}/{file_name}", str_df)
            else:
                with open(dir.joinpath(file_name), "w") as f:
                    f.write(str_df)

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
        if isinstance(dir, Path):
            if dir.is_dir():
                if len(list(dir.iterdir())) > 0:
                    raise ValueError(
                        "The given directory '{dir}' is not empty.".format(
                            dir=str(dir)))
            elif dir.suffix == "":
                dir.mkdir()
            else:
                raise ValueError(
                    "The given directory '{dir}' is not a directory.".format(
                        dir=dir))
        elif not isinstance(dir, zipfile.ZipFile):
            raise ValueError(
                "The given directory '{dir}' is not a directory or zipfile.".format(
                    dir=dir))

        return dir

    @staticmethod
    def _split_date(dates):
        """
        Split datetime into parts.

        Parameters
        ----------
        dates : pandas.DatetimeIndex or list of (datetime.dateime or pandas.Timestamp) or
                pandas.DataFrame of (datetime.datetime or pandas.Timestamp)
            The datetime's to split.

        Returns
        -------
        pandas.DataFrame
            A DataFrame with 5 columns (Jahr, Monat, Tag, Stunde, Minute).
        """
        # if dates is not a list make it a list
        if isinstance(dates, datetime) or isinstance(dates, pd.Timestamp):
            dates = pd.DatetimeIndex([dates])
            index = range(0, len(dates))

        elif isinstance(dates, pd.DatetimeIndex):
            index = dates
        else:
            index = range(0, len(dates))

        # check if date is datetime or Timestamp:
        if not (isinstance(dates[0], pd.Timestamp) or
                isinstance(dates[0], datetime)):
            raise ValueError("Error: The given date is not in a datetime or " +
                            "Timestamp format.")

        return pd.DataFrame(
            {"Jahr": dates.year,
             "Monat": dates.month,
             "Tag": dates.day,
             "Stunde": dates.hour,
             "Minute": dates.minute},
             dtype=int,
             index=index)
