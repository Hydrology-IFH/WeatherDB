import click
import sys
from pathlib import Path
from textwrap import dedent


sys.path.insert(0, Path(__file__).resolve().parent.parent.as_posix())
import weatherdb

# Reusable option decorators
# ---------------------------------------

def period_options(f):
    """Add period options only"""
    f = click.option("--period_from", "-f",
                     default=None, show_default=True,
                     help="The from timestamp of the period to apply the method on. Timestamp should be in the format YYYY-MM-DD.")(f)
    f = click.option("--period_until", "-u",
                     default=None, show_default=True,
                     help="The until timestamp of the period to apply the method on. Timestamp should be in the format YYYY-MM-DD.")(f)
    return f

def stid_option(f):
    """Add station ID options"""
    f = click.option("--stid", "-i",
                     default=["all"], show_default=True, multiple=True,
                     callback=lambda ctx, param, value: "all" if "all" in value else [int(v) for v in value],
                     help="The station IDs to apply the method to. Options are 'all' or a specific station ID. " +
                          "You can enter multiple values to add multiple station IDs.")(f)
    return f

def para_options(f):
    """Add parameter options"""
    f = click.option("--para", "-p",
                     default=["p", "t", "et"], show_default=True, multiple=True,
                     help="The parameters to work with. Options are 'p', 't' and 'et'. " +
                          "You can enter multiple values to add multiple parameters.")(f)
    return f

def common_update_options(f):
    """Add common options for processing commands (para, stid, period)"""
    f = para_options(f)
    f = stid_option(f)
    f = period_options(f)
    return f

# main cli group
# ---------------------------------------

@click.group(help="This is the Command line interface of the WeatherDB package.",
             chain=True,
             context_settings=dict(
                 show_default=True
                 )
            )
@click.version_option(weatherdb.__version__)
@click.option('--do-logging/--no-logging',
              is_flag=True, default=True, show_default=True,
              help="Should the logging be done to the console?")
@click.option('--connection', '-c',
              type=str, default=None, show_default=False,
              help="The connection to use. Default is the value from the configuration file.")
@click.option('--verbose', '-v', # defined here but used in safe_cli
              is_flag=True, default=False, show_default=True,
              help="Should the verbose mode be activated? -> This will print complete tracebacks on errors.")
def cli(do_logging, connection, verbose):
    if do_logging:
        click.echo("logging to console is set on")
        handlers = weatherdb.config.get_list("logging", "handler")
        if "console" not in handlers:
            handlers.append("console")
            weatherdb.config.set("logging", "handler", handlers)

    if connection is not None:
        print(f"setting the connection to {connection}")
        weatherdb.config.set("database", "connection", connection)


def safe_entry():
    try:
        cli()
    except Exception as e:
        if ("-v" in sys.argv) or ("--verbose" in sys.argv):
            raise e
        notes = "\n" + '\n'.join(e.__notes__) if hasattr(e, '__notes__') else ''
        click.echo(f"\033[31;1;4mAn error occurred: {e}{notes}\033[0m", err=True)
        sys.exit(1)

# cli statements to initialize the module
# ---------------------------------------

@cli.command(short_help="Create the database schema for the first time.")
@click.option('--owner', '-o',
              type=str, default=None,
              help=dedent("""
                 The user that should get the ownership of the created tables and schemas.
                 As default is the current user will be the owner."""))
def create_db_schema(owner):
    click.echo("starting to create database schema")
    broker = weatherdb.broker.Broker()
    broker.create_db_schema(owner=owner)


@cli.command(short_help="Use Alembic directly to update or downgrade the database schema. Have a look at the official alembic documentation for more information.")
@click.option('--revision', '-r',
              type=str, default="head", show_default=True,
              help="The revision ID (WeatherDB Version e.g. 'V1.0.2') to upgrade/downgrade to.")
def upgrade_db_schema(revision):
    click.echo("starting to upgrade database schema")
    broker = weatherdb.broker.Broker()
    broker.upgrade_db_schema(revision=revision)


@cli.command(short_help="Create User configuration file.")
@click.option('--file', '-f',
              type=click.Path(resolve_path=True, dir_okay=False, file_okay=True),
              default="ask", show_default=True,
              help="The file to save the user configuration to.")
@click.option('--on-exists', '-e',
              type=str, default="ask", show_default=True,
              help="What to do if the file already exists. Options are 'ask', 'overwrite', 'define' or 'error'.")
def create_user_config(file, on_exists):
    weatherdb.config.create_user_config(user_config_file=file, on_exists=on_exists)


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

    The refined multi annual datasets, that are downloaded are published on Zenodo. [1]_

    References:
    -----------
    .. [1]  Schmit, M.; Weiler, M. (2023). German weather services (DWD) multi annual meteorological rasters for the climate period 1991-2020 refined to 25m grid (1.0.0) [Data set]. Zenodo. `DOI:10.5281/zenodo.10066045 <https://doi.org/10.5281/zenodo.10066045>`_
    """
    click.echo("starting downloading multi annual raster data")
    from weatherdb.utils.get_data import download_ma_rasters
    download_ma_rasters(
        which=which,
        overwrite=overwrite,
        update_user_config=update_user_config)


@cli.command(short_help="Download the needed digital elevation model raster data from Copernicus to the data folder.")
@click.option('--out-dir', '-d',
              type=click.Path(), default=None, show_default=False,
              help="The directory to save the downloaded DEM data to.")
@click.option('--overwrite/--no-overwrite', '-o/-no-o',
              type=bool, is_flag=True, default=None, show_default=False,
              help="Should the digital elevation model raster be downloaded even if it already exists?")
@click.option('--extent', '-e',
              type=tuple, default=(5.3, 46.1, 15.6, 55.4), show_default=True,
              help="The extent in WGS84 of the DEM data to download. The default is the boundary of germany + ~40km.")
@click.option("--update-user-config", "-u",
              type=bool, default=False, show_default=True, is_flag=True,
              help="Should the user configuration be updated with the path to the downloaded DEM?")
@click.option("--service", "-s",
              type=str, default=["prism", "openTopography"], show_default=True, multiple=True,
              help="The service to use to download the DEM. Options are 'prism' or 'openTopography'. " +\
                   "You can use this option muultiple times to test both in the given order until the file could be downloaded.")
def download_dem(out_dir, overwrite, extent, update_user_config, service="prism"):
    """Download the newest DEM data from the Copernicus Sentinel dataset. [1]_

    Only the GLO-30 DEM, wich has a 30m resolution, is downloaded as it is freely available.
    If you register as a scientific researcher also the EEA-10, with 10 m resolution, is available.
    You will have to download the data yourself and define it in the configuration file.

    After downloading the data, the files are merged and saved as a single tif file in the data directory in a subfolder called 'dems'.
    To use the DEM data in the WeatherDB, you will have to define the path to the tif file in the configuration file.

    References:
    -----------
    .. [1] Copernicus DEM - Global and European Digital Elevation Model. Digital Surface Model (DSM) provided in 3 different resolutions (90m, 30m, 10m) with varying geographical extent (EEA: European and GLO: global) and varying format (INSPIRE, DGED, DTED). `DOI:10.5270/ESA-c5d3d65 <https://doi.org/10.5270/ESA-c5d3d65>`_
    """
    click.echo("Starting downloading digital elevation model from Copernicus")
    from weatherdb.utils.get_data import download_dem
    download_dem(
        out_dir=out_dir,
        overwrite=overwrite,
        extent=extent,
        service=service,
        update_user_config=update_user_config)


# cli statements to update the database
# ---------------------------------------
@cli.command(short_help="Update the complete database. Get the newest data from DWD and treat it.")
def update_db():
    click.echo("starting updating the database")
    broker = weatherdb.broker.Broker()
    broker.update_db()


@cli.command(short_help="Update the meta data in the database. Get the newest meta data from DWD.")
def update_meta():
    click.echo("updating the meta data")
    broker = weatherdb.broker.Broker()
    broker.update_meta()


@cli.command(short_help="Update the Richter classes of the precipitation stations in the database.")
def update_richter_class():
    click.echo("starting updating the regionalisation")
    weatherdb.StationsP().update_richter_class()


@cli.command(short_help="Update the multi annual raster values in the database.")
def update_ma_raster():
    click.echo("starting updating the multi annual raster data")
    broker = weatherdb.broker.Broker()
    broker.update_ma_raster()


@cli.command(short_help="Update the raw data of the complete database.")
@common_update_options
def update_raw(para, stid, period_from, period_until):
    click.echo("starting updating the raw data")
    broker = weatherdb.broker.Broker()
    broker.update_raw(paras=para,
                      stids=stid,
                      period=(period_from, period_until))


@cli.command(short_help="Do the quality check of the complete database.")
@common_update_options
def quality_check(para, stid, period_from, period_until):
    click.echo("starting quality check")
    broker = weatherdb.broker.Broker()
    broker.quality_check(paras=para,
                         stids=stid,
                         period=(period_from, period_until))


@cli.command(short_help="Do the filling of the complete database.")
@common_update_options
def fillup(para, stid, period_from, period_until):
    click.echo("starting filling up")
    broker = weatherdb.broker.Broker()
    broker.fillup(paras=para,
                  stids=stid,
                  period=(period_from, period_until))


@cli.command(short_help="Do the richter correction of the complete database.")
@stid_option
@period_options
def richter_correct(stid, period_from, period_until):
    click.echo("starting richter correction")
    broker = weatherdb.broker.Broker()
    broker.richter_correct(stids=stid,
                           period=(period_from, period_until))


# cli admin stuff
# ---------------------------------------

@cli.command(short_help="Set the db version to the current WeatherDB version to prevent recalculation of the whole database. (!!!Only use this if you're sure that the database did all the necessary updates!!!)")
def set_db_version():
    click.echo(dedent(
        """Are you sure you want to set the db version to the current WeatherDB version?
           This will prevent the recalculation of the whole database if there was an update and could resolve in old values in the database."""))
    if click.confirm("Are you sure you want to continue?"):
        click.echo("starting setting db version")
        broker = weatherdb.broker.Broker()
        broker.set_db_version()
    else:
        click.echo("aborting setting db version")

@cli.command(short_help="Forcefully set the active broker flag in the database to deactivated. This is useful if the broker got exited before it could deactivate itself. (!!!Only use this if you're sure that the database did all the necessary updates!!!)")
def force_deactivate_all_broker():
    click.echo(dedent("""
        Are you sure that there is no more broker running? This could lead to problems as multiple brokers could run at the same time contradicting themself."""))
    if click.confirm("Are you sure you want to set the activation flag in the database to deactivated?"):
        click.echo("deactivating the brokers flag")
        broker = weatherdb.broker.Broker()
        broker.force_deactivate_all()
    else:
        click.echo("aborting the deactivation of the brokers flag")

# cli
# ---------------------------------------
if __name__=="__main__":
    safe_entry()

