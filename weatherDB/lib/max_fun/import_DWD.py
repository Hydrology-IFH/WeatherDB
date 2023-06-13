#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A collection of functions to import data from the DWD-CDC Server."""

__author__ = "Max Schmit"
__copyright__ = "Copyright 2021, Max Schmit"

# libraries
import geopandas as gpd
import pandas as pd
from datetime import datetime
from zipfile import ZipFile
import ftplib
import re
from io import BytesIO
import traceback
import logging
from pathlib import Path
import time
import random

from socket import gethostname

# logger
log = logging.getLogger(__name__)
if not log.hasHandlers():
    this_dir = Path(__file__).parent
    log_dir = Path(this_dir).joinpath("logs")
    if not log_dir.is_dir(): log_dir.mkdir()
    log_fh = logging.FileHandler(
        log_dir.joinpath("DWD_import_" + gethostname() + ".txt"))
    log_fh.setLevel(logging.DEBUG)
    log.addHandler(log_fh)

CDC_HOST = "opendata.dwd.de"

# basic functions
# ----------------


def dwd_id_to_str(id):
    """
    Convert a station id to normal DWD format as str.

    Parameters
    ----------
    id : int or str
        The id of the station.

    Returns
    -------
    str
        string of normal DWD Station id.

    """
    return str(int(id) + 100000)[1:6]


def _dwd_date_parser(date_ser):
    """
    Parse the dates from a DWD table to datetime.

    Parameters
    ----------
    date_ser : pd.Series of str or str
        the string from the DWD table. e.g. "20200101" or "2020010112"

    Returns
    -------
    datetime.datetime
        The date as datetime.

    """
    if type(date_ser) != pd.Series:
        raise ValueError("date_str must be a pd.Series of str")
        
    # test if list or single str
    char_num = len(date_ser.iloc[0])

    # parse to correct datetime
    if char_num == 8:
        return pd.to_datetime(date_ser, format='%Y%m%d')
    elif char_num == 10:
        return pd.to_datetime(date_ser, format='%Y%m%d%H')
    elif char_num == 12:
        return pd.to_datetime(date_ser, format='%Y%m%d%H%M')
    else:
        raise ValueError("there was an error while converting the following  to a correct datetime"+ 
                         date_ser.head())


# main functions

def get_dwd_file(zip_filepath):
    """
    Get a DataFrame from one single (zip-)file from the DWD FTP server.

    Parameters
    ----------
    zip_filepath : str
        Path to the file on the server. e.g.

        - "/climate_environment/CDC/observations_germany/climate/10_minutes/air_temperature/recent/10minutenwerte_TU_00044_akt.zip"

        - "/climate_environment/CDC/derived_germany/soil/daily/historical/derived_germany_soil_daily_historical_73.txt.gz"

    Returns
    -------
    pandas.DataFrame
        The DataFrame of the selected file in the zip folder.

    """
    # get the compressed folder from dwd
    with ftplib.FTP(CDC_HOST) as ftp:
        ftp.login()

        # download file
        compressed_bin = BytesIO()
        num_tried = 0
        while num_tried < 10:
            try:
                ftp.retrbinary("RETR " + zip_filepath, compressed_bin.write)
                break
            except Exception as e:
                if num_tried < 9:
                    num_tried += 1
                    time.sleep(random.randint(0,400)/100)
                else:
                    raise e

    # check folder to be derived or observation type import the data
    if re.search("observations", zip_filepath):
        # get zip folder and files
        compressed_folder = ZipFile(compressed_bin)
        compressed_folder_files = compressed_folder.namelist()

        # test if one and only one file matches the pattern
        files = list(filter(re.compile("produkt").search,
                            compressed_folder_files))

        if len(files) == 0:
            raise ValueError(
                "There is no file matching the pattern: produkt " +
                "in the zip files: \n- " +
                "\n- ".join(compressed_folder_files))
        elif len(files) > 1:
            raise ValueError(
                "There are more than one files matching the " +
                "pattern: produkt\nin the zip file: " +
                str(zip_filepath) +
                "\nonly the first file is returned: " +
                str(files[0]))

        # extract the file from the zip folder and return it as pd.DataFrame
        with compressed_folder.open(files[0]) as f:
            df = pd.read_table(f, sep=";",
                             dtype={"Datum":str, "MESS_DATUM":str},
                               skipinitialspace=True,
                               na_values=[-999, -9999, "####", "#####", "######"])

    elif re.search("derived", zip_filepath):
        df = pd.read_table(f"ftp://{CDC_HOST}/{zip_filepath}",
                             compression="gzip",
                             sep=";",
                             skipinitialspace=True,
                             dtype={"Datum":str, "MESS_DATUM":str},
                             na_values=[-999, -9999, "####", "#####", "######"])
    else:
        raise ImportError("ERROR: No file could be imported, as there is " +
                          "just a setup for observation and derived datas")
    
    # convert dates to datetime
    for col in ["MESS_DATUM", "Datum"]:
        if col in df.columns:
            df[col] = _dwd_date_parser(df[col])

    return df

def get_dwd_data(station_id, ftp_folder):
    """
    Get the weather data for one station from the DWD server.

    Parameters
    ----------
    station_id : str or int
        Number of the station to get the weather data from.
    ftp_folder : str
        the base folder where to look for the stations_id file.
        e.g. ftp_folder = "climate_environment/CDC/observations_germany/climate/hourly/precipitation/historical/".
        If the parent folder, where "recent"/"historical" folder is inside, both the historical and recent data gets merged.

    Returns
    -------
    pandas.DataFrame
        The DataFrame of the selected file in the zip folder.

    """
    # check folder to be derived or observation type
    if re.search("observations", ftp_folder):
        station_id = dwd_id_to_str(station_id)
        date_col = "MESS_DATUM"
    elif re.search("derived", ftp_folder):
        station_id = str(int(station_id))
        date_col = "Datum"

    # test if recent or historical specified
    if re.search(r"((observations)|(derived))(?!.*((historical)|(recent))[\/]*$)", ftp_folder):
        if (ftp_folder[-1] != "/"):
            ftp_folder += "/"
        ftp_folders = [ftp_folder + "historical", ftp_folder + "recent"]
    else:
        ftp_folders = [ftp_folder]

    # test if station is in folder or even several times
    comp = re.compile(r".*_" + station_id + r"[_\.].*")
    zipfilenames = []
    with ftplib.FTP(CDC_HOST) as ftp:
        ftp.login()
        for ftp_folder in ftp_folders:
            zipfilenames.extend(list(filter(comp.match, ftp.nlst(ftp_folder))))

    if len(zipfilenames) == 0:
        log.debug("There is no file for the Station " + station_id + " in " +
                  "ftp://opendata.dwd.de/" + ftp_folder)
        return None
    elif len(zipfilenames) > 1:
        log.info("There are several files for the Station " + station_id +
                 " in: \nftp://opendata.dwd.de/" + ftp_folder +
                 "\nthey will get concatenated together! \n" +
                 "These are the files:\n" + "\n".join(zipfilenames))

    # import every file and merge data
    for zipfilename in zipfilenames:
        try:
            if "df_all" not in locals():
                df_all = get_dwd_file(zipfilename)
            else:
                df_new = get_dwd_file(zipfilename)
                # cut out if already in previous file
                df_new = df_new[~df_new[date_col].isin(df_all[date_col])]
                # concatenat the dfs
                df_all = pd.concat([df_all, df_new])
                df_all.reset_index(drop=True, inplace=True)
        except IndexError:
            log.info("The following file could not get imported " +
                        str(zipfilename))

    # check for duplicates in date column
    try:
        df_all.set_index(date_col, inplace=True)
        if df_all.index.has_duplicates:
            df_all = df_all.groupby(df_all.index).mean()
    except:
        pass

    # check if everything worked
    if "df_all" not in locals():
        raise ImportError("The file(s) for the dwd station " +
                          str(station_id) + " couldn't get imported.")

    return df_all

def _concat_dwd_data(df1, df2):
    """Concatenet 2 dwd stations dataframes to one.

    Is usefull for concatenating several dataframes of the same station together
    or historical and recent datas.

    Parameters
    ----------
    df1 : pd.DataFrame
        the first DataFrame of one DWD Station.
    df2 : pd.DataFrame
        the second DataFrame of one DWD Station.

    Returns
    -------
    pd.DataFrame
        a new DataFrame that contain the data of the 2 input DFs.
    """
    # get Date column name
    if "MESS_DATUM" in df1.columns:
        datecolumn = "MESS_DATUM"
    else:
        datecolumn = "Datum"

    # check for which df is oldest
    if df1[datecolumn].min() < df2[datecolumn].min():
        df_old = df1
        df_young = df2
    else:
        df_old = df2
        df_young = df1

    # check if overlapping data
    if df_old[datecolumn].max() > df_young[datecolumn].min():
        df_young = df_young.loc[df_young[datecolumn] > df_old[datecolumn].max()]
    df_concat = pd.concat([df_old, df_young])
    df_concat.reset_index(drop=True, inplace=True)

    return df_concat

def get_dwd_meta(ftp_folder, min_years=0, max_hole_d=9999):
    """
    Get the meta file from the ftp_folder on the DWD server.

    Downloads the meta file of a given folder.
    Corrects the meta file of missing files. So if no file for the station is
    in the folder the meta entry gets deleted.
    Reset "von_datum" in meta file if there is a biger gap than max_hole_d.
    Delets entries with less years than min_years.

    Parameters
    ----------
    ftp_folder : str
        The path to the directory where to search for the meta file.
        e.g. "climate_environment/CDC/observations_germany/climate/hourly/precipitation/recent/".
    min_years : int, optional
        filter the list of stations by a minimum amount of years,
        that they have data for. 0 if the data should not get filtered.
        Only works if the meta file has a timerange defined,
        e.g. in "observations".
        The default is 0.
    max_hole_d : int
        The maximum amount of days missing in the data allowed.
        If there are several files for one station and the time hole is biger
        than this value, the older "von_datum" is overwriten
        in the meta GeoDataFrame.
        The default is 2.

    Returns
    -------
    geopandas.GeoDataFrame
        a GeoDataFrame of the meta file

    """
    # open ftp connection and get list of files in folder
    with ftplib.FTP(CDC_HOST) as ftp:
        ftp.login()

        # get and check the meta_file name
        ftp_files = ftp.nlst(ftp_folder)
        pattern = ".+[(_stations_list)(_Beschreibung_Stationen)].txt"
        meta_file = list(filter(re.compile(pattern).match, ftp_files))

    if len(meta_file) == 0:
        log.info("There is no file matching the pattern: " + pattern +
              "\nin the folder: ftp://opendata.dwd.de/" + str(ftp_folder))
        return None
    elif len(meta_file) > 1:
        log.info("There are more than one files matching the pattern: " +
                  pattern + " in the folder:\nftp://opendata.dwd.de/" +
                  str(ftp_folder) + "\nonly the first file is returned: " +
                  meta_file[0])

    # import meta file
    try:
        if re.search("observations", ftp_folder):
            meta = pd.read_table("ftp://opendata.dwd.de/" + meta_file[0],
                                 skiprows=2, encoding="WINDOWS-1252",
                                 sep=r"\s{2,}|(?<=\d)\s{1}(?=[\w])",  # two or more white spaces or one space after word or digit and followed by word
                                 names=["Stations_id", "von_datum",
                                        "bis_datum", "Stationshoehe",
                                        "geoBreite", "geoLaenge",
                                        "Stationsname", "Bundesland"],
                                 parse_dates=["von_datum", "bis_datum"],
                                 index_col="Stations_id",
                                 engine="python")
        elif re.search("derived", ftp_folder):
            meta = pd.read_table("ftp://opendata.dwd.de/" + meta_file[0],
                                 encoding="WINDOWS-1252", sep=";", skiprows=1,
                                 names=["Stations_id", "Stationshoehe",
                                        "geoBreite", "geoLaenge",
                                        "Stationsname", "Bundesland"],
                                 index_col="Stations_id"
                                 )
    except:
        traceback.print_exc()
        print("URL Error: The URL could not be found:\n" +
              "ftp://opendata.dwd.de/" + meta_file[0])
        return None

    try:
        meta = gpd.GeoDataFrame(meta,
                                geometry=gpd.points_from_xy(meta.geoLaenge,
                                                            meta.geoBreite,
                                                            crs="EPSG:4326"))
        meta = meta.drop(["geoLaenge", "geoBreite"], axis=1)
    except:
        traceback.print_exc()
        print("Error while converting DataFrame to GeoDataFrame," +
              " maybe the columns aren't named 'geoLaenge' and geoBreite'" +
              "\nhere is the header of the DataFrame:\n")
        meta.head()
        return None

    # delete entries where there is no file in the ftp-folder
    rows_drop = []
    str_ftp_files = str(ftp_files)
    for i, row in meta.iterrows():
        if not (re.search(r"[_\.]" + dwd_id_to_str(i) + r"[_\.]|" +
                          r"[_\.]" + str(i) + r"[_\.]", str_ftp_files)):
            rows_drop.append(i)
    meta = meta.drop(rows_drop)

    # change meta date entries if the file has a different date
    if ("observation" in ftp_folder) \
            and ("bis_datum" and "von_datum" in meta) \
            and ("recent" not in ftp_folder):
        zip_files = list(filter(re.compile(".+\d+_\d+_\d+_hist.zip").match,
                                ftp_files))
        zip_files.sort()
        zip_files.append(zip_files[0])  # else the last entry won't get tested
        last_sid, last_from_date, last_to_date = None, None, None
        max_hole_d_td = pd.Timedelta(days=max_hole_d+1)

        for zip_file in zip_files:
            # get new files dates
            filename = zip_file.split("/")[-1]
            _, kind, sid, from_date, to_date, _ = filename.split("_")
            if kind in ["niedereder"]:
                continue
            from_date = pd.Timestamp(from_date)
            to_date = pd.Timestamp(to_date)
            sid = int(sid)

            # compare with previous file's dates
            if last_sid and (sid == last_sid):
                if (from_date - last_to_date) > max_hole_d_td:
                    last_from_date = from_date
                last_to_date = to_date
            else:
                # compare last values with meta file dates
                if last_sid and (last_sid in meta.index):
                    if last_from_date > meta.loc[last_sid, "von_datum"]:
                        meta.loc[last_sid, "von_datum"] = last_from_date
                    if last_to_date < meta.loc[last_sid, "bis_datum"]:
                        meta.loc[last_sid, "bis_datum"] = last_to_date

                # set values as last values
                last_to_date = to_date
                last_from_date = from_date
                last_sid = sid

    # delete entries that do not exceed the minimum amount of years
    if "bis_datum" and "von_datum" in meta:
        days = meta.bis_datum - meta.von_datum
        meta = meta[days >= pd.Timedelta(str(min_years * 365.25) + " d")]
    else:
        log.error("the meta file has no time columns, therefor the table " +
                  "can't get filtered for the available years")

    # return
    return meta


# debug
if False:
    meta = get_dwd_meta(
        ftp_folder="climate_environment/CDC/observations_germany/climate/10_minutes/precipitation/historical/",
        min_years=11, max_hole_d=365)
    print(len(meta))