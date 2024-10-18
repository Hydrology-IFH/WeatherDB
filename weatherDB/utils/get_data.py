"""
Some utilities functions to download the needed data for the module to work.
"""
import requests
from pathlib import Path
from distutils.util import strtobool
import hashlib
import progressbar as pb

from ..config import config

def download_ma_rasters(which="all", overwrite=None, update_user_config=False):
    """Get the multi annual rasters on which bases the regionalisation is done.

    The refined multi annual datasets, that are downloaded are published on Zenodo [1]_

    References
    ----------
    .. [1] Schmit, M.; Weiler, M. (2023). German weather services (DWD) multi annual meteorological rasters for the climate period 1991-2020 refined to 25m grid (1.0.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10066045

    Parameters
    ----------
    which : str or [str], optional
        Which raster to download.
        Options are "dwd", "hyras", "regnie" and "all".
        The default is "all".
    overwrite : bool, optional
        Should the multi annual rasters be downloaded even if they already exist?
        If None the user will be asked.
        The default is None.
    update_user_config : bool, optional
        Should the downloaded rasters be set as the regionalisation rasters in the user configuration file?
        The default is False.
    """
    # DOI of the multi annual dataset
    DOI = "10.5281/zenodo.10066045"

    # check which
    if isinstance(which, str):
        which = [which]
    for w in which:
        if w not in ["all", "dwd", "hyras", "regnie"]:
            raise ValueError(
                "which must be one of 'all', 'dwd', 'hyras' or 'regnie'.")
        if w == "all":
            which = ["dwd", "hyras", "regnie"]
            break

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
        if file_key in which:
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
                    print(f"Skipping {file_key} as overwriting is not allowed.")
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
                line_breaks=False,
                redirect_stdout=True
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

            # update user config
            if update_user_config:
                if config.has_user_config:
                    config.update_user_config(f"data:rasters:{file_key}", "file", str(file_path))
                else:
                    print(f"No user configuration file found, therefor the raster '{file_key}' is not set in the user configuration file.")


def download_dem(overwrite=None, extent=(5.3, 46.1, 15.6, 55.4), update_user_config=False):
    """Download the newest DEM data from the Copernicus Sentinel dataset.

    Only the GLO-30 DEM, which has a 30m resolution, is downloaded as it is freely available.
    If you register as a scientific researcher also the EEA-10, with 10 m resolution, is available.
    You will have to download the data yourself and define it in the configuration file.

    After downloading the data, the files are merged and saved as a single tif file in the data directory in a subfolder called 'DEM'.
    To use the DEM data in the WeatherDB, you will have to define the path to the tif file in the configuration file.

    Source:
    Copernicus DEM - Global and European Digital Elevation Model. Digital Surface Model (DSM) provided in 3 different resolutions (90m, 30m, 10m) with varying geographical extent (EEA: European and GLO: global) and varying format (INSPIRE, DGED, DTED). DOI:10.5270/ESA-c5d3d65.

    Parameters
    ----------
    overwrite : bool, optional
        Should the DEM data be downloaded even if it already exists?
        If None the user will be asked.
        The default is None.
    extent : tuple, optional
        The extent in WGS84 of the DEM data to download.
        The default is the boundary of germany + ~40km = (5.3, 46.1, 15.6, 55.4).
    update_user_config : bool, optional
        Should the downloaded DEM be set as the used DEM in the user configuration file?
        The default is False.
    """
    # import necessary modules
    import rasterio as rio
    from rasterio.merge import merge
    import tarfile
    import shutil
    from tempfile import TemporaryDirectory
    import re
    import json

    # get dem_dir
    base_dir = Path(config.get("data", "base_dir"))
    dem_dir = base_dir / "DEM"
    dem_dir.mkdir(parents=True, exist_ok=True)

    # get available datasets
    prism_url = "https://prism-dem-open.copernicus.eu/pd-desk-open-access/publicDemURLs"
    avl_ds_req = json.loads(
        requests.get(
            prism_url,
            headers={"Accept": "json"}
            ).text
    )
    avl_ds = [{
        "id": e["datasetId"],
        "year": int(e["datasetId"].split("/")[1].split("_")[0]),
        "year_part": int(e["datasetId"].split("/")[1].split("_")[1]),
        "resolution": int(e["datasetId"].split("-")[2]),
        } for e in avl_ds_req]

    # select newest and highest resolution dataset
    ds_id = sorted(
        avl_ds,
        key=lambda x: (-x["resolution"], x["year"], x["year_part"])
        )[-1]["id"]

    # check if dataset already exists
    dem_file = dem_dir / f'{ds_id.replace("/", "__")}.tif'
    if dem_file.exists():
        print(f"The DEM data already exists at {dem_file}.")
        if overwrite is None:
            overwrite = strtobool(input("Do you want to overwrite it? [y/n] "))
        if not overwrite:
            print("Skipping, because overwritting was turned of.")
            return
        else:
            print("Overwriting the dataset.")
    dem_dir.mkdir(exist_ok=True)

    # selecting DEM tiles
    print(f"getting available tiles for Copernicus dataset '{ds_id}'")
    ds_files_req = json.loads(
        requests.get(
            f"{prism_url}/{ds_id.replace('/', '__')}",
            headers={"Accept": "json"}
            ).text
    )
    re_comp = re.compile(r".*/Copernicus_DSM_\d{2}_N\d*_\d{2}_E\d*.*")
    ds_files_all = [
        {"lat": int(Path(f["nativeDemUrl"]).stem.split("_")[3][1:]),
         "long": int(Path(f["nativeDemUrl"]).stem.split("_")[5][1:]),
         **f} for f in ds_files_req if re_comp.match(f["nativeDemUrl"])]
    res_deg = 1
    ds_files = list(filter(
        lambda x: (
            (extent[0] - res_deg) < x["long"] < extent[2] and
            (extent[1] - res_deg) < x["lat"] < extent[3]
            ),
        ds_files_all))

    # download DEM tiles
    print("downloading tiles")
    with TemporaryDirectory() as tmp_dir:
        tmp_dir_fp = Path(tmp_dir)
        for f in pb.progressbar(ds_files):
            with open(tmp_dir_fp / Path(f["nativeDemUrl"]).name, "wb") as d:
                d.write(requests.get(f["nativeDemUrl"]).content)
        print("downloaded all files")

        # extracting tifs from tars
        for i, f in pb.progressbar(list(enumerate(tmp_dir_fp.glob("*.tar")))):
            with tarfile.open(f) as t:
                # extract dem tif
                re_comp = re.compile(r"^.*\/DEM\/.*\.tif$")
                name = list(filter(re_comp.match, t.getnames()))[0]
                with open(tmp_dir_fp/f"{name.split('/')[-1]}", "wb") as d:
                    d.write(t.extractfile(name).read())

                # extract info contract
                if i==0:
                    re_comp = re.compile(r"^.*\/INFO\/.*\.pdf$")
                    name = list(filter(re_comp.match, t.getnames()))[0]
                    with open(tmp_dir_fp/f"{name.split('/')[-1]}", "wb") as d:
                        d.write(t.extractfile(name).read())

            # remove tar
            f.unlink()

        # merge files
        srcs = [rio.open(f) for f in tmp_dir_fp.glob("*.tif")]
        dem_np, dem_tr = merge(srcs)
        dem_meta = srcs[0].meta.copy()
        dem_meta.update({
            "driver": "GTiff",
            "height": dem_np.shape[1],
            "width": dem_np.shape[2],
            "transform": dem_tr
        })
        with rio.open(dem_file, "w", **dem_meta) as d:
            d.write(dem_np)

        # copy info contract
        tmp_eula_fp = next(tmp_dir_fp.glob("*.pdf"))
        shutil.copyfile(
            tmp_eula_fp,
            dem_dir / tmp_eula_fp.name
        )

    print(f"created DEM at '{dem_file}'.")

    # update user config
    if update_user_config:
        if config.has_user_config:
            config.update_user_config("data:rasters", "dems", str(dem_file))
            return
        else:
            print("No user configuration file found, therefor the DEM is not set in the user configuration file.")

    print("To use the DEM data in the WeatherDB, you will have to define the path to the tif file in the user configuration file.")