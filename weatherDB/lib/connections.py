# libraries
import ftplib
import sqlalchemy
import sys, os
from pathlib import Path

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
    # import the secret settings
    try:
        import secretSettings_weatherDB as secrets
    except ImportError:
        # look in parent folders for matching file and insert into path
        this_dir = Path(__file__).parent.resolve()
        for dir in this_dir.parents:
            dir_fp = dir.joinpath("secretSettings_weatherDB.py")
            if dir_fp.is_file():
                with open(dir_fp, "r") as f:
                    if any(["DB_WEA_USER" in l for l in f.readlines()]):
                        sys.path.insert(0, dir.as_posix())
                        break
        try:
            import secretSettings_weatherDB as secrets
        except ImportError:
            raise ImportError("The secretSettings_weatherDB.py file was not found on your system.\n Please put the file somewhere on your PATH directories. For more information see the docs.")
    
    # create the engine
    DB_ENG = sqlalchemy.create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{name}".format(
            user=secrets.DB_WEA_USER,
            pwd=secrets.DB_WEA_PWD,
            host=secrets.DB_HOST,
            name=secrets.DB_WEA_NAME,
            port=secrets.DB_PORT
            )
        )
    del secrets

    # check if user has super user privileges
    with DB_ENG.connect() as con:
        DB_ENG.is_superuser = con.execute("""
            SELECT 'weather_owner' in (
                SELECT rolname FROM pg_auth_members
                LEFT JOIN pg_roles ON oid=roleid
                WHERE member = (SELECT oid FROM pg_roles WHERE rolname='{user}'));
            """.format(user=DB_ENG.url.username)).first()[0]

# decorator function to overwrite methods
def check_superuser(methode):
    def no_super_user(*args, **kwargs):
        raise PermissionError("You are no super user of the Database and therefor this function is not available.")
    if DB_ENG.is_superuser:
        return methode
    else:
        return no_super_user

# DWD - CDC FTP Server
class FTP(ftplib.FTP):
    def login(self, **kwargs):
        # this prevents an error message if the user is already logged in
        try:
            super().login(**kwargs)
        except (ConnectionAbortedError, ftplib.error_temp, BrokenPipeError):
            self.__init__(self.host)
            self.login()
        except (ftplib.error_perm, EOFError):
            pass # this means the connection is already logged in

CDC = FTP("opendata.dwd.de")
CDC.login()

