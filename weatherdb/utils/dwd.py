"""
Some utilities functions to get data from the DWD-CDC server.

Based on `max_fun` package on https://github.com/maxschmi/max_fun
Created by Max Schmit, 2021
"""
# libraries
import dateutil
import ftplib
import pathlib
import geopandas as gpd
import pandas as pd
from zipfile import ZipFile
import re
from io import BytesIO, StringIO
import traceback
import logging
import time
import random

# DWD - CDC FTP Server
CDC_HOST = "opendata.dwd.de"

# logger
log = logging.getLogger(__name__)

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
    return f"{id:0>5}"

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
    if not isinstance(date_ser, pd.Series):
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

# functions
# ---------
def get_ftp_file_list(ftp_conn, ftp_folders):
    """Get a list of files in the folders with their modification dates.

    Parameters
    ----------
    ftp_conn : ftplib.FTP
        Ftp connection.
    ftp_folders : list of str or pathlike object
        The directories on the ftp server to look for files.

    Returns
    -------
    list of tuples of strs
        A list of Tuples. Every tuple stands for one file.
        The tuple consists of (filepath, modification date).
    """
    # check types
    if isinstance(ftp_folders, str):
        ftp_folders = [ftp_folders]
    for i, ftp_folder in enumerate(ftp_folders):
        if isinstance(ftp_folder, pathlib.Path):
            ftp_folders[i] = ftp_folder.as_posix()

    try:
        ftp_conn.voidcmd("NOOP")
    except ftplib.all_errors:
        ftp_conn.connect()

    # get files and modification dates
    files = []
    for ftp_folder in ftp_folders:
        lines = []
        ftp_conn.dir(ftp_folder, lines.append)
        for line in lines:
            parts = line.split(maxsplit=9)
            filepath = ftp_folder + parts[8]
            modtime = dateutil.parser.parse(parts[5] + " " + parts[6] + " " + parts[7])
            files.append((filepath, modtime))

    return files

def get_cdc_file_list(ftp_folders):
    with ftplib.FTP(CDC_HOST) as ftp_con:
        ftp_con.login()
        files = get_ftp_file_list(ftp_con, ftp_folders)
    return files

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

def get_dwd_meta(ftp_folder):
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
        pattern = r".*(?<!_mn4)((_stations_list)|(_Beschreibung_Stationen))+.txt$"
        meta_file = list(filter(re.compile(pattern).match, ftp_files))

    if len(meta_file) == 0:
        log.info(
            f"There is no file matching the pattern '{pattern}'"+
            f"\nin the folder: ftp://{CDC_HOST}/{str(ftp_folder)}")
        return None
    elif len(meta_file) > 1:
        log.info(
            f"There are more than one files matching the pattern: {pattern}" +
            f" in the folder:\nftp://{CDC_HOST}/{str(ftp_folder)}" +
            f"\nonly the first file is returned: {meta_file[0]}")

    # import meta file
    try:
        if re.search("observations", ftp_folder):
            with ftplib.FTP(CDC_HOST) as ftp:
                ftp.login()
                with BytesIO() as bio, StringIO() as sio:
                    ftp.retrbinary("RETR " + meta_file[0], bio.write)
                    sio.write(bio.getvalue().decode("WINDOWS-1252").replace("\r\n", "\n"))
                    colnames = sio.getvalue().split("\n")[0].split()
                    sio.seek(0)
                    meta = pd.read_table(
                        sio,
                        skiprows=2,
                        lineterminator="\n",
                        sep=r"\s{2,}|(?<=\d|\))\s{1}(?=[\w])",  # two or more white spaces or one space after digit and followed by word
                        names=colnames,
                        parse_dates=[col for col in colnames if "datum" in col.lower()],
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
        print(meta.head())
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
        zip_files = list(filter(re.compile(r".+\d+_\d+_\d+_hist.zip").match,
                                ftp_files))
        zip_files.sort()
        zip_files.append(zip_files[0])  # else the last entry won't get tested
        last_sid, last_from_date, last_to_date = None, None, None

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

    # trim whitespace in string columns
    for dtype, col in zip(meta.dtypes, meta.columns):
        if pd.api.types.is_string_dtype(dtype) and col != "geometry":
            meta[col] = meta[col].str.strip()

    # return
    return meta