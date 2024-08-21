import click
import sys
from pathlib import Path

sys.path.insert(0, Path(__file__).resolve().parent.parent.as_posix())
import weatherDB

@click.group(help="This is the Command line interface of the weatherDB package.",
             chain=True)
@click.option('--do-logging/--no-logging',
              is_flag=True, default=True, show_default=True,
              help="Should a Log-file be written?")
@click.option('--connection', '-c',
              type=str, default=None,
              help="The connection to use. Default is the value from the configuration file.")
def cli(do_logging, connection=None):
    if do_logging:
        click.echo("logging is on")
        weatherDB.setup_file_logging()
    else:
        weatherDB.setup_file_logging(False)

    if connection is not None:
        weatherDB.config.set("database", "connection", connection)

@cli.command(short_help="Update the complete database. Get the newest data from DWD and treat it.")
def update_db():
    click.echo("starting updating the database")
    broker = weatherDB.broker.Broker()
    broker.update_db()

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

@cli.command(short_help="Set the db version to the current weatherDB version. (!!!Only use this if you're sure that the database did all the necessary updates!!!)")
def set_db_version():
    click.echo("starting setting db version")
    broker = weatherDB.broker.Broker()
    broker.set_db_version()

# cli
if __name__=="__main__":
    cli()

