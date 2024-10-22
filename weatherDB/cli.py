import click
import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.parent.as_posix())
import weatherDB

@click.group(help="This is the Command line interface of the weatherDB package.",
             chain=True)
@click.option('--do-logging/--no-logging',
              is_flag=True, default=True, show_default=True,
              help="Should the logging be done to the console?")
@click.option('--connection', '-c',
              type=str, default=None, show_default=False,
              help="The connection to use. Default is the value from the configuration file.")
def cli(do_logging, connection=None):
    if do_logging:
        click.echo("logging to console is set on")
        handlers = weatherDB.config.getlist("logging", "handler")
        if "console" not in handlers:
            handlers.append("console")
            weatherDB.config.set("logging", "handler", handlers)

    if connection is not None:
        print(f"setting the connection to {connection}")
        weatherDB.config.set("database", "connection", connection)

@cli.command(short_help="Update the complete database. Get the newest data from DWD and treat it.")
def update_db():
    click.echo("starting updating the database")
    broker = weatherDB.broker.Broker()
    broker.update_db()

@cli.command(short_help="Update the meta data in the database. Get the newest meta data from DWD.")
def update_meta():
    click.echo("updating the meta data")
    broker = weatherDB.broker.Broker()
    broker.update_meta()

@cli.command(short_help="Update the Richter classes of the precipitation stations in the database.")
def update_richter_class():
    click.echo("starting updating the regionalisation")
    weatherDB.StationsP().update_richter_class()

@cli.command(short_help="Update the multi annual raster values in the database.")
def update_ma_raster():
    click.echo("starting updating the multi annual raster data")
    broker = weatherDB.broker.Broker()
    broker.update_ma_raster()

@cli.command(short_help="Update the raw data of the complete database.")
def update_raw():
    click.echo("starting updating the raw data")
    broker = weatherDB.broker.Broker()
    broker.update_raw()

@cli.command(short_help="Do the quality check of the complete database.")
def quality_check():
    click.echo("starting quality check")
    broker = weatherDB.broker.Broker()
    broker.quality_check()

@cli.command(short_help="Do the filling of the complete database.")
def fillup():
    click.echo("starting filling up")
    broker = weatherDB.broker.Broker()
    broker.fillup()

@cli.command(short_help="Do the richter correction of the complete database.")
def richter_correct():
    click.echo("starting richter correction")
    broker = weatherDB.broker.Broker()
    broker.richter_correct()

@cli.command(short_help="Create the database schema for the first time.")
def create_db_schema():
    click.echo("starting to create database schema")
    broker = weatherDB.broker.Broker()
    broker.create_db_schema()

@cli.command(short_help="Create User configuration file.")
@click.option('--file', '-f',
              type=str, default="ask", show_default=True,
              help="The file to save the user configuration to.")
def create_user_config(file):
    weatherDB.config.create_user_config(user_config_file=file)

@cli.command(short_help="Set the db version to the current weatherDB version to prevent recalculation of the whole database. (!!!Only use this if you're sure that the database did all the necessary updates!!!)")
def set_db_version():
    click.echo("starting setting db version")
    broker = weatherDB.broker.Broker()
    broker.set_db_version()

@cli.command(short_help="Download the needed multi-annual raster data from zenodo to the data folder.")
@click.option('--overwrite', '-o',
              type=bool, default=None, show_default=True,
              help="Should the multi annual rasters be downloaded even if they already exist?")
@click.option('--which', '-w',
              type=str, default=["all"], show_default=True,
              multiple=True,
              help="Which raster to download. Options are 'dwd', 'hyras', 'regnie' or 'all'.")
@click.option("--update-user-config", "-u",
              type=bool, default=False, show_default=True, is_flag=True,
              help="Should the user configuration be updated with the path to the downloaded rasters?")
def download_ma_rasters(which, overwrite, update_user_config):
    """Get the multi annual rasters on which bases the regionalisation is done.

    The refined multi annual datasets, that are downloaded are published on Zenodo:
    Schmit, M.; Weiler, M. (2023). German weather services (DWD) multi annual meteorological rasters for the climate period 1991-2020 refined to 25m grid (1.0.0) [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10066045
    """
    click.echo("starting downloading multi annual raster data")
    from weatherDB.utils.get_data import download_ma_rasters
    download_ma_rasters(overwrite=overwrite)

@cli.command(short_help="Download the needed digital elevation model raster data from Copernicus to the data folder.")
@click.option('--overwrite/--no-overwrite', '-o/-no-o',
              type=bool, is_flag=True, default=None, show_default=False,
              help="Should the digital elevation model raster be downloaded even if it already exists?")
@click.option('--extent', '-e',
              type=tuple, default=(5.3, 46.1, 15.6, 55.4), show_default=True,
              help="The extent in WGS84 of the DEM data to download. The default is the boundary of germany + ~40km.")
@click.option("--update-user-config", "-u",
              type=bool, default=False, show_default=True, is_flag=True,
              help="Should the user configuration be updated with the path to the downloaded DEM?")
def download_dem(overwrite, extent):
    """Download the newest DEM data from the Copernicus Sentinel dataset.

    Only the GLO-30 DEM, wich has a 30m resolution, is downloaded as it is freely available.
    If you register as a scientific researcher also the EEA-10, with 10 m resolution, is available.
    You will have to download the data yourself and define it in the configuration file.

    After downloading the data, the files are merged and saved as a single tif file in the data directory in a subfolder called 'dems'.
    To use the DEM data in the WeatherDB, you will have to define the path to the tif file in the configuration file.

    Source:
    Copernicus DEM - Global and European Digital Elevation Model. Digital Surface Model (DSM) provided in 3 different resolutions (90m, 30m, 10m) with varying geographical extent (EEA: European and GLO: global) and varying format (INSPIRE, DGED, DTED). DOI:10.5270/ESA-c5d3d65.
    """
    click.echo("Starting downloading digital elevation model from Copernicus")
    from weatherDB.utils.get_data import download_dem
    download_dem(overwrite=overwrite, extent=extent)

# cli
if __name__=="__main__":
    cli()

