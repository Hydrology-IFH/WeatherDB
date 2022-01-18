# libraries
import ftplib

# DB connection
try:
    from aldjemy.core import get_engine
    DB_ENG = get_engine("weather")
except:
    # if not started inside of django environment
    import sqlalchemy
    import sys, os
    from pathlib import Path

    # find secret settings path and insert into path
    for dir in Path(__file__).parent.resolve().parents:
        if dir.joinpath("secretSettings.py").is_file():
            sys.path.insert(0, dir.as_posix())
            break
    # sys.path.insert(0, Path(__file__).resolve().parents[3].as_posix())

    # from main.settings import DATABASES

    # DB_ENG = sqlalchemy.create_engine(
    #     "postgresql://{user}:{pwd}@{host}:{port}/{name}".format(
    #         user=DATABASES["weather"]["USER"],
    #         pwd=DATABASES["weather"]["PASSWORD"],
    #         host=DATABASES["weather"]["HOST"],
    #         name=DATABASES["weather"]["NAME"],
    #         port=DATABASES["weather"]["PORT"]
    #         )
    #     )
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
    ###################################
    ## here something must get changed to also work without django environment

# check if user has super user privileges
with DB_ENG.connect() as con:
    DB_ENG.is_superuser = con.execute("""
        SELECT 'weather_owner' in (
            SELECT rolname FROM pg_auth_members
            LEFT JOIN pg_roles ON oid=roleid
            WHERE member = (SELECT oid FROM pg_roles WHERE rolname='{user}'));
        """.format(user=DB_ENG.url.username)).first()[0]

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

