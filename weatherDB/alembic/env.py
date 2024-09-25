from alembic import context
import re

import weatherDB as wdb
from weatherDB.db.models import ModelBase
from weatherDB.db.connections import db_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# add your model's MetaData object here
# for 'autogenerate' support
# in console do: weatherDB>alembic -c alembic\alembic.ini revision --autogenerate -m "comment" --rev-id "V1.0.0"
target_metadata = ModelBase.metadata

# check for alembic database copnnection in the weatherDB config
# ##############################################################
engine = config.attributes.get("engine", None)
if wdb.config.has_section('database:alembic') and engine is None:
    print("Setting the database connection to the users configuration of 'database:alembic'.")
    wdb.config.set('database', 'connection', "alembic")
    engine = db_engine.engine

# get other values from config
# ############################
exclude_tables = re.sub(
    r"\s+",
    '',
    config.get_main_option('exclude_tables', '')
).split(',')
valid_schemas = ModelBase.metadata._schemas
valid_tables = {
    schema: [table.name
             for table in ModelBase.metadata.tables.values()
             if table.schema == schema]
    for schema in ModelBase.metadata._schemas}

def include_name(name, type_, parent_names, *args,**kwargs):
    if type_ == "schema":
        return (name in valid_schemas) or (name is None)
    else:
        schema = parent_names["schema_name"] if parent_names["schema_name"] is not None else "public"
        if schema not in valid_schemas:
            return False

        if type_ == "table":
            return name in valid_tables.get(schema, []) and name not in exclude_tables

        table = parent_names["table_name"]
        return table in valid_tables.get(schema, []) and table not in exclude_tables


# migration functions
# ###################
def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            include_schemas=True,
            include_name=include_name
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    raise NotImplementedError("offline mode is not supported")
else:
    run_migrations_online()
