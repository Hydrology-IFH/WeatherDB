# libraries
import sqlalchemy
from sqlalchemy import text as sqltxt
from sqlalchemy import URL
import functools

from ..config import config

# DB connection
###############
class DBEngine:
    def __init__(self):
        self._reset_engine()

        # add the listeners for configuration changes
        self._config_listener_connection = (
            "database",
            "connection",
            self._connection_update)
        self._config_listener_subsection = (
            f"database:{config.get('database', 'connection')}",
            None,
            self._reset_engine)
        config.add_listener(*self._config_listener_connection)
        config.add_listener(*self._config_listener_subsection)

    def __del__(self):
        config.remove_listener(*self._config_listener_connection)
        config.remove_listener(*self._config_listener_subsection)

    def _connection_update(self):
        """Listener if another database subsection is selected."""
        # first remove the old section listener
        config.remove_listener(*self._config_listener_subsection)

        # add the new section listener
        self._config_listener_subsection = (
            f"database:{config.get('database', 'connection')}",
            None,
            self._reset_engine)
        config.add_listener(*self._config_listener_subsection)

        # create a new engine
        self.create_engine()

    # Engine and Session
    # ##################
    def _reset_engine(self):
        """Reset the engine to None to force a recreation on next usage."""
        self._engine = None
        self._session = None
        self._is_superuser = None
        self._select_privilege = None
        self._create_privilege = None
        self._update_privilege = None
        self._insert_privilege = None
        self._delete_privilege = None
        self._priviledge_to_recheck = set()

    def connect(self, *args, **kwargs):
        return self.get_engine().connect(*args, **kwargs)

    def get_engine(self):
        """Get the sqlalchemy database engine.

        Returns the last created engine if possible or creates a new one.

        Returns
        -------
        sqlalchemy.engine.base.Engine
            _description_
        """
        if self._engine is None:
            self.create_engine()

        return self._engine

    def create_engine(self):
        # create the engine
        con_key = config.get("database", "connection")
        con_sect_key = f"database:{con_key}"
        con_section = config[con_sect_key]
        user, pwd = config.get_db_credentials()
        self._engine = sqlalchemy.create_engine(
            URL.create(
                drivername="postgresql+psycopg2",
                username=user,
                password=pwd,
                host=con_section["HOST"],
                database=con_section["DATABASE"],
                port=con_section["PORT"]),
            connect_args={
                "options": "-c timezone=utc",
                'connect_timeout': 10})
        self._check_is_superuser()

        return self._engine

    @property
    def engine(self):
        return self.get_engine()

    @engine.setter
    def engine(self, value):
        raise PermissionError("You are not allowed to change the engine of the database connection.\nPlease change the configuration to change the database connection.")

    def create_session(self):
        """Create a new session from the engine.

        Returns
        -------
        sqlalchemy.orm.session.Session
            _description_
        """
        self._session = sqlalchemy.orm.sessionmaker(bind=self.get_engine())
        return self._session

    def get_session(self):
        """Get a session from the engine.

        Returns
        -------
        sqlalchemy.orm.session.Session
            _description_
        """
        if self._session is None:
            return self.create_session()
        else:
            return self._session

    @property
    def session(self):
        return self.get_session()

    @session.setter
    def session(self, value):
        raise PermissionError("You are not allowed to change the session of the database connection.\nPlease change the configuration to change the database connection.")

    def reload_config(self):
        """Reload the configuration and create a new engine.
        """
        self.create_engine()

    # Privilege checks
    ##################
    def _check_is_superuser(self):
        with self.connect() as con:
            self._is_superuser = con.execute(sqltxt(
                "SELECT usesuper FROM pg_user WHERE usename = current_user;"
            )).first()[0]
        return self._is_superuser

    @property
    def is_superuser(self):
        if self._is_superuser is None:
            self._check_is_superuser()
        return self._is_superuser

    @is_superuser.setter
    def is_superuser(self, value):
        raise PermissionError("You are not allowed to change the superuser status of the database connection, as this is due to PostGreSQL database privileges of your database user.")

    def _check_privilege(self, privilege):
        if self._is_superuser:
            return True
        else:
            with self.connect() as con:
                if privilege == "CREATE":
                    return con.execute(sqltxt(
                        f"SELECT pg_catalog.has_schema_privilege('public', '{privilege}');"
                    )).first()[0]
                res = con.execute(sqltxt(
                    f"""SELECT bool_and(pg_catalog.has_table_privilege(table_name, '{privilege}'))
                       FROM information_schema.TABLES
                       WHERE table_schema=CURRENT_SCHEMA AND table_type='BASE TABLE';"""
                )).first()[0]
                if res is None:
                    if privilege in self._priviledge_to_recheck:
                        self.log.debug(f"Privilege {privilege} not found, probably because database isn't initiated. Setting as True.")
                    else:
                        self._priviledge_to_recheck.add(privilege)
                    return True
                else:
                    if privilege in self._priviledge_to_recheck:
                        self._priviledge_to_recheck.remove(privilege)
                    return res

    def _check_select_privilege(self):
        """Check on the database if the user has the SELECT privilege."""
        self._select_privilege = self._check_privilege("SELECT")
        return self._select_privilege

    def _check_update_privilege(self):
        """Check on the database if the user has the UPDATE privilege."""
        self._update_privilege = self._check_privilege("UPDATE")
        return self._update_privilege

    def _check_insert_privilege(self):
        """Check on the database if the user has the INSERT privilege."""
        self._insert_privilege = self._check_privilege("INSERT")
        return self._insert_privilege

    def _check_create_privilege(self):
        """Check on the database if the user has the CREATE privilege."""
        self._create_privilege = self._check_privilege("CREATE")
        return self._create_privilege

    def _check_delete_privilege(self):
        """Check on the database if the user has the DELETE privilege."""
        self._delete_privilege = self._check_privilege("DELETE")
        return self._delete_privilege

    @property
    def select_privilege(self):
        """Does the user have the PostGreSQL SELECT privilege on the database?"""
        if self._select_privilege is None or "SELECT" in self._priviledge_to_recheck:
            self._check_select_privilege()
        return self._select_privilege

    @property
    def update_privilege(self):
        """Does the user have the PostGreSQL UPDATE privilege on the database?"""
        if self._update_privilege is None or "UPDATE" in self._priviledge_to_recheck:
            self._check_update_privilege()
        return self._update_privilege

    @property
    def insert_privilege(self):
        """Does the user have the PostGreSQL INSERT privilege on the database?"""
        if self._insert_privilege is None or "INSERT" in self._priviledge_to_recheck:
            self._check_insert_privilege()
        return self._insert_privilege

    @property
    def upsert_privilege(self):
        """Does the user have the PostGreSQL INSERT and UPDATE privilege on the database?"""
        return self.insert_privilege and self.update_privilege

    @property
    def create_privilege(self):
        """Does the user have the PostGreSQL CREATE privilege on the database?"""
        if self._create_privilege is None or "CREATE" in self._priviledge_to_recheck:
            self._check_create_privilege()
        return self._create_privilege

    @property
    def delete_privilege(self):
        """Does the user have the PostGreSQL DELETE privilege on the database?"""
        if self._delete_privilege is None or "DELETE" in self._priviledge_to_recheck:
            self._check_delete_privilege()
        return self._delete_privilege

    @property
    def all_privileges(self):
        """Does the user have all (SELECT, UPDATE, INSERT, DELETE, CREATE) PostGreSQL privileges on the database?"""
        return self.select_privilege and \
            self.update_privilege and \
            self.insert_privilege and \
            self.create_privilege and \
            self.delete_privilege

    def _privilege_setters(self, property_name):
        raise PermissionError(f"You are not allowed to change the value of {property_name} as this is due to PostGreSQL database privileges of your user.")

    @select_privilege.setter
    def select_privilege(self, value):
        self._privilege_setters("select_privilege")

    @update_privilege.setter
    def update_privilege(self, value):
        self._privilege_setters("update_privilege")

    @insert_privilege.setter
    def insert_privilege(self, value):
        self._privilege_setters("insert_privilege")

    @upsert_privilege.setter
    def upsert_privilege(self, value):
        self._privilege_setters("upsert_privilege")

    @create_privilege.setter
    def create_privilege(self, value):
        self._privilege_setters("create_privilege")

    @delete_privilege.setter
    def delete_privilege(self, value):
        self._privilege_setters("delete_privilege")

    @all_privileges.setter
    def all_privileges(self, value):
        self._privilege_setters("all_privileges")

    def deco_is_superuser(self, target):
        """Decorator to check if the user is a superuser."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.is_superuser:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You are no super user of the Database and therefor this function is not available.")
        return wrapper

    def deco_select_privilege(self, target):
        """Decorator to check if the user has the SELECT privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.select_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You have no select privilege on the database and therefor this function is not available.")
        return wrapper

    def deco_update_privilege(self, target):
        """Decorator to check if the user has the UPDATE privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.update_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You have no update privilege on the database and therefor this function is not available.")
        return wrapper

    def deco_insert_privilege(self, target):
        """Decorator to check if the user has the INSERT privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.insert_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You have no insert privilege on the database and therefor this function is not available.")
        return wrapper

    def deco_upsert_privilege(self, target):
        """Decorator to check if the user has the INSERT and UPDATE privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.upsert_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You have no upsert privilege on the database and therefor this function is not available.")
        return wrapper

    def deco_create_privilege(self, target):
        """Decorator to check if the user has the CREATE privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.create_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You are no admin user of the Database and therefor this function is not available.")
        return wrapper

    def deco_delete_privilege(self, target):
        """Decorator to check if the user has the DELETE privilege."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.delete_privilege:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You have no delete privilege on the database and therefor this function is not available.")
        return wrapper

    def deco_all_privileges(self, target):
        """Decorator to check if the user has all (SELECT, UPDATE, INSERT, DELETE, CREATE) privileges."""
        @functools.wraps(target)
        def wrapper(*args, **kwargs):
            if self.all_privileges:
                return target(*args, **kwargs)
            else:
                raise PermissionError("You are no super user of the Database and therefor this function is not available.")
        return wrapper


db_engine = DBEngine()
