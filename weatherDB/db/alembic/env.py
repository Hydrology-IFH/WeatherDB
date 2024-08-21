from alembic import context
import re

from ..connections import db_engine

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# add your model's MetaData object here
# for 'autogenerate' support
# in console do: weatherDB\db>alembic revision --autogenerate -m "comment" --rev-id "V1.0.0"
from ..models import Base
target_metadata = Base.metadata


# get other values from config
# ############################
# def exclude_tables_from_config(config_):
#     tables_ = config_.get("tables", None)
#     if tables_ is not None:
#         tables = tables_.split(",")
#     return tables

# Changes from: https://gist.github.com/utek/6163250#gistcomment-3851168
exclude_tables = re.sub(r"\s+", '',  # replace whitespace
                    config.get_main_option('exclude', '')).split(',')
print(config.get_main_option('exclude', ''))

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
