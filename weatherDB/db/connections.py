# libraries
import sqlalchemy
from sqlalchemy import text as sqltxt
from sqlalchemy import URL
import os
from ..config.config import config, get_db_credentials

# DB connection
###############
class DB_ENGINE:
    _engine = None
    _is_superuser = None

    def connect(self, *args, **kwargs):
        return self.get_db_engine().connect(*args, **kwargs)

    def get_db_engine(self):
        if self._engine is not None:
            return self._engine

        # check if in Sphinx creation mode
        if "RTD_documentation_import" in os.environ:
            from mock_alchemy.mocking import UnifiedAlchemyMagicMock
            self._engine = UnifiedAlchemyMagicMock()
            self.is_superuser = True
        elif "WEATHERDB_MODULE_INSTALLING" in os.environ:
            self._engine = type("test", (), {})()
            self.is_superuser = True
        else:
            # create the engine
            user, pwd = get_db_credentials()
            self._engine = sqlalchemy.create_engine(URL.create(
                drivername="postgresql+psycopg2",
                username=user,
                password=pwd,
                host=config["database"]["HOST"],
                database=config["database"]["DATABASE"],
                port=config["database"]["PORT"]
                ))

            del user, pwd

            self.check_is_superuser()
        return self._engine

    def check_is_superuser(self):
        with self.connect() as con:
            self._is_superuser = con.execute(sqltxt("""
                    SELECT count(*)!=0 FROM pg_auth_members pam
                    WHERE member = (SELECT oid FROM pg_roles WHERE rolname='{user}')
                        AND roleid = (SELECT oid FROM pg_roles WHERE rolname='weather_owner');
                """.format(user=self._engine.url.username))).first()[0]
        return self._is_superuser

    @property
    def is_superuser(self):
        if self._is_superuser is None:
            self.check_is_superuser()
        return self._is_superuser

    @is_superuser.setter
    def is_superuser(self, value):
        raise PermissionError("You are not allowed to change the superuser status of the database connection.")

db_engine = DB_ENGINE()

# decorator function to overwrite methods
def check_superuser(methode):
    def no_super_user(*args, **kwargs):
        raise PermissionError("You are no super user of the Database and therefor this function is not available.")
    if DB_ENGINE.is_superuser:
        return methode
    else:
        return no_super_user

