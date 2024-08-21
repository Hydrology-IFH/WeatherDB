"""
The configuration module for the weatherDB module.
"""
import configparser
from pathlib import Path
import keyring
from getpass import getpass
import shutil
import sqlalchemy as sa

__all__ = ["config"]

# create the config parser class
class ConfigParser(configparser.ConfigParser):
    _DEFAULT_CONFIG_FILE = Path(__file__).parent.resolve()/'config_default.ini'
    _MAIN_CONFIG_FILE = Path(__file__).parent.resolve()/'config_main.ini'

    def __init__(self, *args, **kwargs):
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
        self.load_user_config(raise_error=False)

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
        value : str, int or bool
            The new value for the option.
        """
        if section not in self.sections():
            self.add_section(section)
        if option not in self[section] or (value != self.get(section, option)):
            prev_value = self.get(section, option, fallback=None)

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
        value : str, int or bool
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

    def getlist(self, section, option):
        """Get a list of values from a configuration option.

        This function parses the configuration option seperated by commas and returns a list of values."""
        return [v.strip() for v in self.get(section, option).replace("\n", "").split(",")]

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
        db_sect = self[f"database.{db_key}"]
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
            user = input(f"Please enter the username for the database '{db_sect.get('host')}\{db_sect.get('database')}': ")
        if password is None:
            password = getpass(f"Please enter the password for the user {user} on the database '{db_sect.get('host')}\{db_sect.get('database')}': ")

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
        self._set(f"database.{db_key}", "USER", user)
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

    def create_user_config(self, user_config_file=None):
        """Create a new user config file.

        Parameters
        ----------
        user_config_file : str or Path, optional
            The path to the new user config file.
            If not given, the function will ask for it.
            The default is None.
        """
        if user_config_file is None:
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

        # copy the default config file to the user config file
        shutil.copyfile(self._DEFAULT_CONFIG_FILE, user_config_file)

        self._set("main", "user_config_file", str(user_config_file))

        print(f"User config file created at {user_config_file}")
        print("Please edit the file to your needs and reload user config with load_user_config() or by reloading the module.")

    def load_user_config(self, raise_error=True):
        """(re)load the user config file.
        """
        if user_config_file:=self.get("main", "user_config_file", fallback=False):
            if Path(user_config_file).exists():
                with open(user_config_file) as f:
                    f_cont = f.read()
                    if "PASSWORD" in f_cont:
                        raise PermissionError(
                            "For security reasons the password isn't allowed to be in the config file.\nPlease use set_db_credentials to set the password.")
                    self.read_string(f_cont)
                # if "PASSWORD" in config["database"]:
                #     raise PermissionError(
                #         "For security reasons the password isn't allowed to be in the config file.\nPlease use set_db_credentials to set the password.")
            else:
                raise FileNotFoundError(
                    f"User config file not found at {user_config_file}.\nPlease set the user config file with set_user_config_file.")
        elif raise_error:
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


# initiate the config parser
config = ConfigParser()