"""
The configuration module for the weatherDB module.
"""
import configparser
from pathlib import Path
import keyring
from getpass import getpass
import sqlalchemy as sa
import textwrap
import re
import os
from datetime import datetime, timezone

# create the config parser class
class ConfigParser(configparser.ConfigParser):
    """This is the class/object of the configurations for the weatherDB module.
    """
    _DEFAULT_CONFIG_FILE = Path(__file__).parent.resolve()/'config_default.ini'
    _MAIN_CONFIG_FILE = Path(__file__).parent.resolve()/'config_main.ini'

    def __init__(self, *args, **kwargs):
        """
        Warning
        -------
        You shouldn't initialize this class directly, but use the :py:data:``weatherDB.config`` object from the weatherDB module, which is an instance of this class.
        """
        super().__init__(
            interpolation=configparser.ExtendedInterpolation(),
            *args,
            **kwargs)

        self._system_listeners = [
            # (section_name, option_name, callback_function)
            ("main", "user_config_file", self._write_main_config)
        ]
        self._user_listeners = []

        # read the configuration files
        self.read(self._DEFAULT_CONFIG_FILE)
        self._read_main_config()
        self.load_user_config(raise_undefined_error=False)
        self.load_environment_variables()

    def add_listener(self, section, option, callback):
        """Add a callback function to be called when a configuration option is changed.

        Parameters
        ----------
        section : str
            The section of the configuration file.
            If None, the callback will be called for every change.
        option : str
            The option of the configuration file.
            If None, the callback will be called for every change in the given section.
        callback : function
            The function to be called when the configuration option is changed.
        """
        if (section, option, callback) not in self._user_listeners:
            self._user_listeners.append((section, option, callback))

    def remove_listener(self, section, option, callback="_all_"):
        """Remove a callback function from the list of callbacks.

        Parameters
        ----------
        section : str or None
            The section of the configuration file.
            If "_all_", the callback will be removed for every change.
        option : str or None
            The option of the configuration file.
            If "_all_", the callback will be removed for every change in the given section.
        callback : function or str, optional
            The function to be removed from the list of callbacks.
            If "_all_", all callbacks for the given section and option will be removed.
            The default is "_all_".
        """
        drop_cbs = []
        for cb in self._user_listeners:
            if (section == "_all_") or (cb[0] == section):
                if (option == "_all_") or (cb[1] == option):
                    if (callback is None) or (cb[2] == callback):
                        drop_cbs.append(cb)
        for cb in drop_cbs:
            self._user_listeners.remove(cb)

    def _read_main_config(self):
        self.read(self._MAIN_CONFIG_FILE)
        self._set(
            "main",
            "module_path",
            Path(__file__).resolve().parent.parent.as_posix())

    def _write_main_config(self):
        with open(self._MAIN_CONFIG_FILE, "w+") as fp:
            self._write_section(
                fp=fp,
                section_name="main",
                section_items=self._sections["main"].items(),
                delimiter=self._delimiters[0])

    def _set(self, section, option, value):
        """The internal function to set a configuration option for the weatherDB module.

        Please use set instead.

        Parameters
        ----------
        section : str
            A section of the configuration file.
            See config_default.ini for available sections.
        option : str
            The option to be changed.
            See config_default.ini for available options and explanations.
        value : str, int, bool or list
            The new value for the option.
        """
        if section not in self.sections():
            self.add_section(section)
        if isinstance(value, list):
            value = ",\n\t".join([str(val) for val in value])
        if option not in self[section] or (value.replace("\t", "") != self.get(section, option)):
            super().set(section, option, value)

            # fire the change listeners
            for cb_section, cb_option, cb in self._system_listeners + self._user_listeners:
                if (cb_section is None or cb_section == section):
                    if (cb_option is None or cb_option == option):
                        cb()

    def set(self, section, option, value):
        """Set a configuration option for the weatherDB module.

        Parameters
        ----------
        section : str
            A section of the configuration file.
            See config_default.ini for available sections.
        option : str
            The option to be changed.
            See config_default.ini for available options and explanations.
        value : str, int, bool or list
            The new value for the option.

        Raises
        ------
        PermissionError
            If you try to change the database password with this method.
            Use set_db_credentials instead.
        """
        if "database" in section and option == "PASSWORD":
            raise PermissionError(
                "It is not possible to change the database password with set, please use config.set_db_credentials.")

        self._set(section, option, value)

    def get_list(self, section, option):
        """Get a list of values from a configuration option.

        This function parses the configuration option seperated by commas and returns a list of values."""
        if raw_value:= self.get(section, option, fallback=None):
            return [v.strip()
                    for v in raw_value.replace("\n", "").split(",")
                    if len(v.strip())>0]
        return []

    def getlist(self, section, option):
        """Get a list of values from a configuration option.

        This function parses the configuration option seperated by commas and returns a list of values.

        Warning
        -------
        This function will become deprecated in the future. Please use get_list instead."""
        import warnings
        warnings.warn("getlist will become deprecated, please use get_list instead.", FutureWarning)
        return self.get_list(section, option)

    def get_datetime(self, section, option, fallback=None):
        """Get a date from a configuration option.

        This function parses the configuration option and returns a datetime object."""
        if raw_value:= self.get(section, option, fallback=fallback):
            return datetime.strptime(raw_value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        return None

    def get_date(self, section, option, fallback=None):
        """Get a date from a configuration option.

        This function parses the configuration option and returns a date object."""
        return self.get_datetime(section, option, fallback=fallback).date()

    def _get_db_key_section(self, db_key=None):
        """Get the database section for the weatherDB database.

        Parameters
        ----------
        db_key : str, optional
            The key/name for the database section in the configuration file.
            If not given, the function will use the default database connection.
            The default is None.

        Returns
        -------
        str, str, configparser.SectionProxy
            The connection key, keyring_key and the connection configuration section.
        """
        if db_key is None:
            db_key = self.get("database", "connection")
        db_sect = self[f"database:{db_key}"]
        return db_key, f"weatherDB_{db_sect.get('host')}", db_sect

    def set_db_credentials(
            self,
            db_key=None,
            user=None,
            password=None):
        """Set the database credentials for the weatherDB database.

        Parameters
        ----------
        db_key : str, optional
            The key/name for the database section in the configuration file.
            If not given, the function will use the default database connection.
            The default is None.
        user : str, optional
            The username for the database.
            If not given, the function will take the user from configuration if possible or ask for it.
            The default is None.
        password : str, optional
            The password for the database user.
            If not given, the function will ask for it.
            The default is None.
        """
        # get connection section and keys
        db_key, keyring_key, db_sect = self._get_db_key_section(db_key=db_key)

        # check if user is given
        if user is None and (user:=db_sect.get("USER")) is None:
            user = input(f"Please enter the username for the database '{db_sect.get('host')}\\{db_sect.get('database')}': ")
        if password is None:
            password = getpass(f"Please enter the password for the user {user} on the database '{db_sect.get('host')}\\{db_sect.get('database')}': ")

        # test connection
        try:
            con_url = sa.URL.create(
                drivername="postgresql+psycopg2",
                username=user,
                password=password,
                host=db_sect["HOST"],
                database=db_sect["DATABASE"],
                port=db_sect["PORT"]
            )
            with sa.create_engine(con_url).connect() as con:
                con.execute(sa.text("SELECT 1;"))
        except Exception as e:
            print(f"Connection failed, therefor the settings are not stored: {e}")
            return

        # remove old password
        try:
            keyring.delete_password(
                keyring_key,
                db_sect["USER"])
        except:
            pass

        # set new credentials
        self._set(f"database:{db_key}", "USER", user)
        keyring.set_password(keyring_key, user, password)

    def get_db_credentials(self, db_key=None):
        """Get the database credentials for the weatherDB database.

        Parameters
        ----------
        db_key : str, optional
            The key/name for the database section in the configuration file.
            If not given, the function will use the default database connection.
            The default is None.

        Returns
        -------
        str, str
            The username and the password for the database.
        """
        db_key, keyring_key, db_sect = self._get_db_key_section(db_key)

        if "user" not in db_sect or not keyring.get_password(keyring_key, db_sect["user"]):
            print("No database credentials found. Please set them.")
            self.set_db_credentials()

        return db_sect["user"], keyring.get_password(keyring_key, db_sect["user"])

    @property
    def has_user_config(self):
        """Check if a user config file is defined.

        Returns
        -------
        bool
            True if a user config file is defined, False otherwise.
        """
        return self.has_option("main", "user_config_file") or "WEATHERDB_USER_CONFIG_FILE" in os.environ

    @property
    def user_config_file(self):
        """Get the path to the user config file.

        Returns
        -------
        str or None
            The path to the user config file.
        """
        if self.has_user_config:
            if user_config := self.get("main", "user_config_file", fallback=None):
                return user_config
            return os.environ.get("WEATHERDB_USER_CONFIG_FILE", None)
        return None

    def create_user_config(self, user_config_file=None, on_exists="ask"):
        """Create a new user config file.

        Parameters
        ----------
        user_config_file : str or Path, optional
            The path to the new user config file.
            If not given, the function will use the config.user_config_file if available or ask for it.
            If set to "ask", the function will allways open a filedialog to select the file.
            The default is None.
        on_exists : str, optional
            What to do if the user config file already exists.
            The options are:
            - "ask"/"A" : Ask the user what to do.
            - "overwrite"/"O" : Overwrite the existing file.
            - "define"/"D" : Only define the file as new user config file location.
            - "error"/"E" : Raise an error and stop the creation.
            The default is "ask".

        """
        if user_config_file is None:
            if self.has_user_config:
                user_config_file = self.user_config_file
            else:
                user_config_file = "ask"

        # ask for the user config file
        if user_config_file == "ask":
            try:
                from tkinter import Tk
                from tkinter import filedialog
                tkroot = Tk()
                tkroot.attributes('-topmost', True)
                tkroot.iconify()
                user_config_file = filedialog.asksaveasfilename(
                    defaultextension=".ini",
                    filetypes=[("INI files", "*.ini")],
                    title="Where do you want to save the user config file?",
                    initialdir=Path("~").expanduser(),
                    initialfile="WeatherDB_config.ini",
                    confirmoverwrite=True,
                )
                tkroot.destroy()
            except ImportError:
                while True:
                    user_input = input("Please enter the path to the user config file: ")
                    if user_input.lower() in ["exit", "quit"] or user_input == "":
                        print("Quiting the user config creation.")
                        return
                    user_config_file = Path(user_input)
                    user_input = input("Please enter the path to the user config file: ")
                    if user_input.lower() in ["exit", "quit"] or user_input == "":
                        print("Quiting the user config creation.")
                        return
                    user_config_file = Path(user_input)
                    if user_config_file.parent.exists():
                        if user_config_file.suffix != ".ini":
                            print("The file has to be an INI file.")
                            continue
                        break
                    else:
                        print("Invalid path. Please try again.")

        # check if file exists
        write = True
        if Path(user_config_file).exists():
            msg = f"User config file already exists at {user_config_file}."
            if on_exists[0].upper() == "E":
                raise FileExistsError(msg)

            # get input from user
            print(msg)
            if on_exists[0].upper() == "A":
                on_exists = input(
                    "What do you want to do with the existing file?"+
                    "[overwrite/define/exit] (first letter is enough): ").upper()[0]

            # treat the user input
            if on_exists == "O":
                write = True
            elif on_exists== "D":
                write = False
            elif on_exists == "E":
                return
            else:
                raise ValueError("Invalid value for on_exists. Please try again.")

        # copy the default config file to the user config file
        if write:
            with open(user_config_file, "w") as user_f, \
                open(self._DEFAULT_CONFIG_FILE, "r") as def_f:
                for line in def_f.readlines():
                    if not re.match(r"^\[|;", line):
                        line = "; " + line
                    user_f.write(line)
            print(f"User config file created at {user_config_file}")
            print("Please edit the file to your needs and reload user config with load_user_config() or by reloading the module.")

        # set the user config file in the main config
        self._set("main", "user_config_file", str(user_config_file))
        print("The user config file location got set in main config.")

    def load_user_config(self,
                         raise_undefined_error=True,
                         if_not_existing=os.environ.get("WEATHERDB_HANDLE_NON_EXISTING_CONFIG", "ask")):
        """(re)load the user config file.

        Parameters
        ----------
        raise_undefined_error : bool, optional
            Raise an error if no user config file is defined.
            The default is True.
        if_not_existing : str, optional
            What to do if the user config file is not existing at the specified location.
            The options are:
            - "ask" : Ask the user what to do.
            - "ignore" : Ignore the error and continue.
            - "create" : Create a new user config file.
            - "define" : Define a new user config file location.
            - "remove" : Remove the user config file location.
            The default is the value of the environment variable "WEATHERDB_HANDLE_NON_EXISTING_CONFIG" or if undefined "ask".
        """
        if self.has_user_config:
            user_config_file = self.user_config_file
            if Path(user_config_file).exists():
                with open(user_config_file) as f:
                    f_cont = f.read()
                    if "PASSWORD" in f_cont:
                        raise PermissionError(
                            "For security reasons the password isn't allowed to be in the config file.\nPlease use set_db_credentials to set the password.")
                    self.read_string(f_cont)
            else:
                print(f"User config file not found at {user_config_file}.")
                # get user decision what to do
                if if_not_existing.lower() == "ask":
                    print(textwrap.dedent("""
                        What do you want to do:
                        - [R] : Remove the user config file location
                        - [D] : Define a new user config file location
                        - [C] : Create a new user config file with default values
                        - [I] : Ignore error"""))
                    while True:
                        user_dec = input("Enter the corresponding letter: ").upper()
                        if user_dec in ["R", "D", "C", "I"]:
                            break
                        else:
                            print("Invalid input. Please try again and use one of the given letters.")
                else:
                    user_dec = if_not_existing[0].upper()

                # do the user decision
                if user_dec == "R":
                    self.remove_option("main", "user_config_file")
                elif user_dec == "D":
                    self.set_user_config_file()
                elif user_dec == "C":
                    self.create_user_config()
        elif raise_undefined_error:
            raise FileNotFoundError("No user config file defined.")

    def set_user_config_file(self, user_config_file=None):
        """Define the user config file.

        Parameters
        ----------
        user_config_file : str, Path or None, optional
            The path to the user config file.
            If None, the function will open a filedialog to select the file.
            The default is None.
        """
        if user_config_file is None:
            from tkinter import Tk
            from tkinter import filedialog
            tkroot = Tk()
            tkroot.attributes('-topmost', True)
            tkroot.iconify()
            user_config_file = filedialog.askopenfilename(
                defaultextension=".ini",
                filetypes=[("INI files", "*.ini")],
                title="Select the User configuration file",
                initialdir=Path("~").expanduser(),
                initialfile="WeatherDB_config.ini"
                )
            tkroot.destroy()

        if not Path(user_config_file).exists():
            raise FileNotFoundError(
                f"User config file not found at {user_config_file}")

        self._set("main", "user_config_file", str(user_config_file))
        self.load_user_config()

    def update_user_config(self, section, option, value):
        """Update a specific value in the user config file.

        Parameters
        ----------
        section : str
            The section of the configuration file.
        option : str
            The option of the configuration file.
        value : str, int, bool or list
            The new value for the option.

        Raises
        ------
        ValueError
            If no user config file is defined.
        """
        # check if user config file is defined
        if not self.has_user_config:
            raise ValueError("No user config file defined.\nPlease create a user config file with create_user_config() or define an existiing user configuration file with set_user_config_file, before updating the user config.")

        # update the value in the config
        self.set(section, option, value)
        value = self.get(section, option)

        # update the value in the user config file
        section = section.replace(".",":").lower()
        option = option.upper()
        value_set = False
        with open(self.user_config_file, "r") as f:
            ucf_lines = f.readlines()
            for commented in [False, True]:
                in_section = False
                for i, line in enumerate(ucf_lines):
                    line_c = line.strip().lower()

                    # get section change
                    if re.match(r"\[.*\]", line_c):
                        if in_section:
                            if not value_set and commented:
                                print(i)
                                print("Option not found in section and is therefor added at the end of the section.")
                                ucf_lines.insert(i, f"; Option added by config.update_user_config-call.\n{option} = {value}\n\n")
                                value_set = True
                            break

                        in_section = line_c.startswith(f"[{section}]")

                    # set value if option is found
                    if commented:
                        re_comp = re.compile(f"(;\\s*){option.lower()}\\s*=")
                    else:
                        re_comp = re.compile(f"{option.lower()}\\s*=")
                    if in_section and re_comp.match(line_c):
                        # check if multiline option
                        j = 0
                        while i+j<len(ucf_lines) and \
                            ucf_lines[i+j].strip(";").split(";")[0].strip().endswith(","):
                            j += 1

                        # remove the old additional values
                        for k in range(i+1, i+j+1):
                            ucf_lines[k] = ""

                        # set the value
                        ucf_lines[i] = f"{option} = {value}\n"
                        value_set = True
                        break
                if value_set:
                    break

        # add the option if not found in the section
        if not value_set:
            print("Section not found and is therefor added at the end of the file.")
            ucf_lines.append(textwrap.dedent(f"""

                [{section}]
                ; Option and section added by config.update_user_config-call.
                {option} = {value}"""))

        # write the new user config file
        with open(self.user_config_file, "w") as f:
            f.writelines(ucf_lines)

    def load_environment_variables(self):
        """Load the environment variables into the configuration.

        The following environment variables are possible to use:
        - WEATHERDB_USER_CONFIG_FILE : The path to the user config file.
        - WEATHERDB_HANDLE_NON_EXISTING_CONFIG : What to do if the user config file is not existing at the specified location.
        - WEATHERDB_DB_USER : The username for the database.
        - WEATHERDB_DB_PASSWORD : The password for the database user.
        - WEATHERDB_DB_HOST : The host for the database.
        - WEATHERDB_DB_PORT : The port for the database.
        - WEATHERDB_DB_DATABASE : The database name.
        - WEATHERDB_DATA_BASE_DIR : The base path for the data directory.
        - WEATHERDB_LOGGING_HANDLER : The logging handler to use. Possible values are "console" and "file".
        - WEATHERDB_LOGGING_LEVEL : The logging level to use. Possible values are "DEBUG", "INFO", "WARNING", "ERROR" and "CRITICAL".
        - WEATHERDB_LOGGING_DIRECTORY : The directory to store the log files.
        - WEATHERDB_LOGGING_FILE : The file name for the log file.
        - WEATHERDB_HORIZON_RADIUS : The radius in meters for the horizon angle calculation.
        - WEATHERDB_HORIZON_CRS : The CRS as EPSG code for the distance calculation during the horizon angle calculation.
        """
        # database connection variables
        db_vars = ["WEATHERDB_DB_USER", "WEATHERDB_DB_PASSWORD", "WEATHERDB_DB_HOST", "WEATHERDB_DB_PORT", "WEATHERDB_DB_DATABASE"]
        var_exists = [var for var in db_vars if var in os.environ ]
        if len(var_exists)==len(db_vars):
            self.set(
                "database:environment_variables",
                "host",
                os.environ.get("WEATHERDB_DB_HOST"))
            self.set(
                "database:environment_variables",
                "port",
                os.environ.get("WEATHERDB_DB_PORT"))
            self.set(
                "database:environment_variables",
                "database",
                os.environ.get("WEATHERDB_DB_DATABASE"))

            # get password from file if it is a path, to work with docker secrets
            password = os.environ.get("WEATHERDB_DB_PASSWORD")
            if Path(password).exists():
                with open(password, "r") as f:
                    password = f.read().strip()

            self.set_db_credentials(
                "environment_variables",
                os.environ["WEATHERDB_DB_USER"],
                password)
            self.set("database", "connection", "environment_variables")
        elif len(var_exists)>0:
            print(textwrap.dedent(f"""
                Only some database environment variables are set ({', '.join(var_exists)}).
                To configure your database with environment variables all needed variables are needed.
                Please set the following missing environment variables:"""))
            for var in db_vars:
                if var not in var_exists:
                    print(f" - {var}")

        # other environment variable settings
        for env_key, (section, option) in {
                "WEATHERDB_DATA_BASE_DIR": ("data", "base_dir"),
                "WEATHERDB_LOGGING_HANDLER": ("logging", "handlers"),
                "WEATHERDB_LOGGING_LEVEL": ("logging", "level"),
                "WEATHERDB_LOGGING_DIRECTORY": ("logging", "directory"),
                "WEATHERDB_LOGGING_FILE": ("logging", "file"),
                "WEATHERDB_HORIZON_RADIUS": ("weatherdb", "horizon_radius"),
                "WEATHERDB_HORIZON_CRS": ("weatherdb", "horizon_crs"),
                }.items():
            if env_key in os.environ:
                self.set(section, option, os.environ.get(env_key))