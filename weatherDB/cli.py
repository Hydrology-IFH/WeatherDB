import click
import sys, os
sys.path.insert(
    0,
    os.path.split(os.path.abspath(os.path.split(__file__)[0]))[0])
import weatherDB

@click.command()
def update_db():
    weatherDB.setup_file_logging()
    broker = weatherDB.broker.Broker()
    broker.update_db()

# cli
if __name__=="__main__":
    click.echo("starting updating the database")
    update_db()

