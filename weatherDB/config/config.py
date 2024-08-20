import configparser
from pathlib import Path
import keyring
from getpass import getpass
import shutil

# set the file paths for the config files
DEFAULT_CONFIG_FILE = Path(__file__).parent/'config_default.ini'
SYS_CONFIG_FILE = Path(__file__).parent/'config_sys.ini' # set by this module

# read the default configuration file
config = configparser.ConfigParser(
    interpolation=configparser.ExtendedInterpolation(),
    converters={
        "list": lambda x: [v.strip() for v in x.replace("\n", "").split(",")]}
)
config.read(DEFAULT_CONFIG_FILE)

# read the system configuration file
config.read(SYS_CONFIG_FILE)

# define functions
# ----------------

def save_config():
    """Save the current configuration to the system configuration file.
    """
    with open(SYS_CONFIG_FILE, 'w') as configfile:
        config.write(configfile)

# setting configuration
def _set_config(section, option, value):
    """The internal function to set a configuration option for the weatherDB module.

    Please use set_config instead.

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
    if section not in config.sections():
        config.add_section(section)
    if option not in config[section] or (value != config.get(section, option)):
        config.set(section, option, value)
        save_config()

# changing configuration
def set_config(section, option, value):
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
        If you try to change the databse credentials with this function.
        Use set_db_credentials instead.
    """
    if section =="database" and (option == "PASSWORD" or option == "USER"):
        raise PermissionError("It is not possible to change the database credentials with set_config, please use set_db_credentials.")

    _set_config(section, option, value)

def set_db_credentials(user=None, password=None):
    """Set the database credentials for the weatherDB database.

    Parameters
    ----------
    user : str, optional
        The username for the database.
        If not given, the function will ask for it.
        The default is None.
    password : str, optional
        The password for the database user.
        If not given, the function will ask for it.
        The default is None.
    """
    # remove old password
    try:
        keyring.delete_password("weatherDB", config["database"]["USER"])
    except:
        pass

    # check if user is given
    if user is None:
        user = input("Please enter the username for the database: ")
    if password is None:
        password = getpass("Please enter the password for the given database user: ")

    # set new credentials
    _set_config("database", "USER", user)
    keyring.set_password("weatherDB", user, password)

def get_db_credentials():
    """Get the database credentials for the weatherDB database.

    Returns
    -------
    str, str
        The username and the password for the database.
    """
    if "user" not in config["database"]:
        print("No database credentials found. Please set them.")
        set_db_credentials()

    user = config["database"]["user"]
    password = keyring.get_password("weatherDB", user)

    return user, password

def init_user_config(user_config_file=None):
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
    shutil.copyfile(DEFAULT_CONFIG_FILE, user_config_file)

    _set_config("main", "user_config_file", str(user_config_file))

    print(f"User config file created at {user_config_file}")
    print("Please edit the file to your needs and reload user config with load_user_config() or by reloading the module.")

def load_user_config(raise_error=True):
    """(re)load the user config file.
    """
    if user_config_file:=config.get("main", "user_config_file", fallback=False):
        if Path(user_config_file).exists():
            config.read(user_config_file)
            if "PASSWORD" in config["database"]:
                raise PermissionError("For security reasons the password isn't allowed to be in the config file. Please use set_db_credentials to set the password.")
            save_config()
        else:
            raise FileNotFoundError(f"User config file not found at {user_config_file}")
    elif raise_error:
        raise FileNotFoundError("No user config file defined.")

load_user_config(raise_error=False)