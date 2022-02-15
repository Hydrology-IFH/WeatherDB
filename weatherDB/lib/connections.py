# libraries
import ftplib
import sqlalchemy
import sys, os
from pathlib import Path

# DB connection
###############
# find secret settings path and insert into path
this_dir = Path(__file__).parent.resolve()
secret_found = False
for dir in this_dir.parents:
    if dir.joinpath("secretSettings.py").is_file():
        sys.path.insert(0, dir.as_posix())
        secret_found = True
        break

# find example settings for the documentation
if not secret_found and "RTD_documentation_import" in os.environ:
    from mock_alchemy.mocking import UnifiedAlchemyMagicMock
    DB_ENG = UnifiedAlchemyMagicMock()
    DB_ENG.is_superuser = True
else:

    # import the secret settings
    import secretSettings as secrets

    DB_ENG = sqlalchemy.create_engine(
        "postgresql://{user}:{pwd}@{host}:{port}/{name}".format(
            user=secrets.DB_WEA_USER,
            pwd=secrets.DB_WEA_PWD,
            host=secrets.DB_HOST,
            name=secrets.DB_WEA_NAME,
            port=secrets.DB_PORT
            )
        )

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

