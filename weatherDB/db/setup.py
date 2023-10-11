from alembic.config import Config
from alembic import command
from .connections import DB_ENG

def migrate_db():
    # Run Alembic migrations to update the database schema
    alembic_cfg = Config("alembic/alembic.ini")
    command.upgrade(alembic_cfg, "head")
