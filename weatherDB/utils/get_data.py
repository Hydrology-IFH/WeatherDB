"""
Some utilities functions to download the needed data for the module to work.
"""
from ..config import config
import requests
from pathlib import Path
from distutils.util import strtobool
import hashlib
import progressbar as pb

def download_ma_rasters(which="all", overwrite=None):
    """Get the multi annual rasters on which bases the regionalisation is done.

    The refined multi annual datasets, that are downloaded are published on Zenodo:
    Schmit, M.; Weiler, M. (2023). German weather services (DWD) multi annual meteorological rasters for the climate period 1991-2020 refined to 25m grid (1.0.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10066045

    Parameters
    ----------
    which : str
        Which raster to download.
        Options are "dwd", "hyras", "regnie" and "all".
        The default is "all".
    overwrite : bool, optional
        Should the multi annual rasters be downloaded even if they already exist?
        If None the user will be asked.
        The default is None.
    """
    # DOI of the multi annual dataset
    DOI = "10.5281/zenodo.10066045"

    # check which
    if which not in ["all", "dwd", "hyras", "regnie"]:
        raise ValueError(
            "which must be one of 'all', 'dwd', 'hyras' or 'regnie'.")

    # get zenodo record
    zenodo_id = requests.get(
        f"https://doi.org/{DOI}"
    ).url.split("/")[-1]
    zenodo_rec = requests.get(
        f"https://zenodo.org/api/records/{zenodo_id}"
    ).json()

    # download files
    for file in zenodo_rec["files"]:
        file_key = file["key"].lower().split("_")[0].split("-")[0]
        if which == "all" or which == file_key:
            # check if file is in config
            if f"data:rasters:{file_key}" not in config:
                print(f"Skipping {file_key} as it is not in your configuration.\nPlease add a section 'data:rasters:{file_key}' to your configuration file.")
                continue

            # check if file already exists
            file_path = Path(config.get(f"data:rasters:{file_key}", "file"))
            if file_path.exists():
                skip = False
                if overwrite is False:
                    skip = True
                elif overwrite is None:
                    skip = not strtobool(input(
                        f"{file_key} already exists at {file_path}.\n"+
                        "Do you want to overwrite it? [y/n] "))

                if skip:
                    print(f"Skipping {file_key} as overwritting is not allowed.")
                    continue

            # check if the directory exists
            if not file_path.parent.exists():
                if strtobool(input(
                        f"The directory \"{file_path.parent}\" does not exist.\n"+
                        "Do you want to create it? [y/n] ")):
                    file_path.parent.mkdir(parents=True)

            # download file
            r = requests.get(file["links"]["self"], stream=True)
            if r.status_code != 200:
                r.raise_for_status()  # Will only raise for 4xx codes, so...
                raise RuntimeError(
                    f'Request to {file["links"]["self"]} returned status code {r.status_code}')
            block_size = 1024
            file_size = int(r.headers.get('Content-Length', 0))
            pbar = pb.ProgressBar(
                max_value=file_size,
                prefix=f"downloading {file_key}: ",
                widgets=[ " ",
                    pb.widgets.DataSize(),
                    "/",
                    pb.widgets.DataSize("max_value"),
                    pb.widgets.AdaptiveTransferSpeed(
                        format='(%(scaled)5.1f %(prefix)s%(unit)-s/s) '),
                    pb.widgets.Bar(), " ",
                    pb.widgets.Percentage(),
                    pb.widgets.ETA()],
                line_breaks=False
                ).start()
            md5 = hashlib.md5()
            with open(file_path, "wb+") as f:
                for i, chunk in enumerate(r.iter_content(block_size)):
                    f.write(chunk)
                    md5.update(chunk)
                    pbar.update(i*block_size)
            pbar.finish()

            # check checksum
            if md5.hexdigest() != file["checksum"].replace("md5:", ""):
                raise ValueError(
                    f"Checksum of {file_key} doesn't match. File might be corrupted.")


