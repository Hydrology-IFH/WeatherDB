import click
import sys
sys.path.insert(0, ".")
import weatherDB

@click.command()
def update_db():
    broker = weatherDB.broker.Broker()
    broker.update_db()

# cli
if __name__=="__main__":
    update_db()

