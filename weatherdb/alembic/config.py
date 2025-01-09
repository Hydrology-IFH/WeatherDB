from alembic.config import Config
from pathlib import Path

from ..config import config
from ..db.connections import db_engine

config = Config(
    Path(config.get("main", "module_path"))/"alembic"/"alembic.ini",
    attributes={"engine": db_engine.get_engine()})