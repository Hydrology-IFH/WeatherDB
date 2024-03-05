# libraries
import sqlalchemy
from sqlalchemy import text as sqltxt
import os
from ..config.config import config

# DB connection
###############
# check if in Sphinx creation mode
if "RTD_documentation_import" in os.environ:
    from mock_alchemy.mocking import UnifiedAlchemyMagicMock
    DB_ENG = UnifiedAlchemyMagicMock()
    DB_ENG.is_superuser = True
elif "WEATHERDB_MODULE_INSTALLING" in os.environ:
    DB_ENG = type("test", (), {"is_superuser":False})()
else:
    # create the engine
    DB_ENG = sqlalchemy.create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{database}".format(
            user=config["database"]["USER"],
            pwd=config["database"]["PASSWORD"],
            host=config["database"]["HOST"],
            database=config["database"]["DATABASE"],
            port=config["database"]["PORT"]
            )
        )

    # check if user has super user privileges
    with DB_ENG.connect() as con:
        DB_ENG.is_superuser = con.execute(sqltxt("""
                SELECT count(*)!=0 FROM pg_auth_members pam
                WHERE member = (SELECT oid FROM pg_roles WHERE rolname='{user}')
                    AND roleid = (SELECT oid FROM pg_roles WHERE rolname='weather_owner');
            """.format(user=DB_ENG.url.username))).first()[0]

# decorator function to overwrite methods
def check_superuser(methode):
    def no_super_user(*args, **kwargs):
        raise PermissionError("You are no super user of the Database and therefor this function is not available.")
    if DB_ENG.is_superuser:
        return methode
    else:
        return no_super_user

# DWD - CDC FTP Server
CDC_HOST = "opendata.dwd.de"

