from alembic import context
import re

import weatherDB as wdb
from weatherDB.db.models import Base

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# add your model's MetaData object here
# for 'autogenerate' support
# in console do: weatherDB>alembic -c alembic\alembic.ini revision --autogenerate -m "comment" --rev-id "V1.0.0"
target_metadata = Base.metadata

# check for alembic database copnnection in the weatherDB config
# ##############################################################
db_engine = config.attributes.get("engine", None)
if wdb.config.has_section('database:alembic') and db_engine is None:
    print("Setting the database connection to the users configuration of 'database:alembic'.")
    wdb.config.set('database', 'connection', "alembic")
    db_engine = wdb.db.get_engine()

# get other values from config
# ############################
exclude_tables = re.sub(
    r"\s+",
    '',
    config.get_main_option('exclude_tables', '')
).split(',')

def include_object(object, name, type_, *args, **kwargs):
    return not (type_ == 'table' and name in exclude_tables)

# migration functions
# ###################
def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    with db_engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_object=include_object
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise NotImplementedError("offline mode is not supported")
else:
    run_migrations_online()
